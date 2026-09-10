/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake

import io.airbyte.cdk.command.OpaqueStateValue
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.discover.EmittedField
import io.airbyte.cdk.read.DefaultJdbcCursorIncrementalPartition
import io.airbyte.cdk.read.DefaultUnsplittableJdbcCursorIncrementalPartition
import io.airbyte.cdk.read.DefaultJdbcPartition
import io.airbyte.cdk.read.DefaultJdbcSharedState
import io.airbyte.cdk.read.DefaultJdbcStreamState
import io.airbyte.cdk.read.DefaultJdbcStreamStateValue
import io.airbyte.cdk.read.DefaultJdbcUnsplittableSnapshotPartition
import io.airbyte.cdk.read.DefaultJdbcUnsplittableSnapshotWithCursorPartition
import io.airbyte.cdk.output.DataChannelMedium
import io.airbyte.cdk.output.OutputMessageRouter
import io.airbyte.cdk.output.sockets.FieldValueEncoder
import io.airbyte.cdk.output.sockets.toJson
import io.airbyte.cdk.read.FieldValueChange
import io.airbyte.cdk.read.JdbcSharedState
import io.airbyte.cdk.read.JdbcStreamState
import io.airbyte.cdk.read.ResourceType
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.JdbcConcurrentPartitionsCreator
import io.airbyte.cdk.read.JdbcCursorPartition
import io.airbyte.cdk.read.JdbcNonResumablePartitionReader
import io.airbyte.cdk.read.JdbcPartition
import io.airbyte.cdk.read.JdbcPartitionFactory
import io.airbyte.cdk.read.JdbcPartitionReader
import io.airbyte.cdk.read.JdbcPartitionsCreator
import io.airbyte.cdk.read.JdbcPartitionsCreatorFactory
import io.airbyte.cdk.read.MODE_PROPERTY
import io.airbyte.cdk.read.PartitionReadCheckpoint
import io.airbyte.cdk.read.PartitionReader
import io.airbyte.cdk.read.Sample
import io.airbyte.cdk.read.SelectQuerier
import io.airbyte.cdk.read.SelectQuery
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/*
 * Large single-query reads.
 *
 * Snowflake stages large query results as chunks in cloud storage, downloaded via credentials
 * minted when the query completes and never refreshed by the JDBC driver (observed lifetime
 * ~6h). A partition whose single result set takes longer than that to consume fails with
 * `Max retry reached for the download of chunk#N ... HTTP status=403`, makes no forward
 * progress, and every retry repeats the same read. How long a result set takes to consume is
 * set by the slowest stage of the pipeline (typically the destination), so the only robust
 * defence is to keep every individual query's result set small.
 *
 * This connector therefore subdivides any read estimated above [SAFE_QUERY_BYTES] with
 * `MOD(ABS(HASH(col, ...)), n) = i` predicates into n disjoint, jointly-complete result sets,
 * each consumed well within the credential lifetime. This applies regardless of partition shape
 * (cold-start snapshots with or without a primary key, cursor-incremental windows) and
 * regardless of cursor cardinality. State handling differs by shape:
 *  - Cursor-incremental windows run as parallel bucket readers. `FeedReader` publishes
 *    checkpoints serially in partition-list order, so all buckets except the last report the
 *    window lower bound (no forward progress) and only the last reports the upper bound: a
 *    failure anywhere resumes from the original window. Duplicates possible, data loss not.
 *  - Snapshots run sequentially inside ONE reader with a single end-of-partition checkpoint,
 *    because no state encoding exists for "snapshot partially complete". Crash semantics are
 *    unchanged from stock (restart the snapshot); each query is bounded so the read completes.
 */

/** Appends the hash-bucket predicate to a generated query, WHERE-aware.
 *
 * Hashes the explicit column list: `HASH(*)` is only allowed in a SELECT clause in Snowflake,
 * whereas `HASH(col, ...)` is an ordinary scalar call, legal in WHERE. */
internal fun hashBucketQuery(q: SelectQuery, bucketIndex: Int, bucketCount: Int): SelectQuery {
    require(q.columns.isNotEmpty()) { "cannot hash-bucket a query with no columns" }
    val hashArgs: String = q.columns.joinToString(", ") { "\"${it.id}\"" }
    val predicate: String = "MOD(ABS(HASH($hashArgs)), $bucketCount) = $bucketIndex"
    val sql: String =
        if (q.sql.contains(" WHERE ")) "${q.sql} AND $predicate" else "${q.sql} WHERE $predicate"
    return SelectQuery(sql, q.columns, q.bindings)
}

/** Number of buckets needed to keep each query's result set within [SnowflakeHashBucketPartitionsCreator.SAFE_QUERY_BYTES]. */
internal fun hashBucketCount(expectedByteSize: Long): Int {
    val safe: Long = SnowflakeHashBucketPartitionsCreator.SAFE_QUERY_BYTES
    return ((expectedByteSize + safe - 1) / safe)
        .coerceIn(2L, SnowflakeHashBucketPartitionsCreator.MAX_BUCKET_COUNT.toLong())
        .toInt()
}

/** One of [bucketCount] hash buckets subdividing a cursor-incremental partition's window. */
class SnowflakeHashBucketPartition(
    val parent: JdbcCursorPartition<DefaultJdbcStreamState>,
    val cursor: EmittedField,
    val cursorLowerBound: JsonNode,
    val bucketIndex: Int,
    val bucketCount: Int,
) : JdbcPartition<DefaultJdbcStreamState> {

    override val streamState: DefaultJdbcStreamState = parent.streamState

    override val nonResumableQuery: SelectQuery
        get() = hashBucketQuery(parent.nonResumableQuery, bucketIndex, bucketCount)

    override val completeState: OpaqueStateValue
        get() =
            if (bucketIndex < bucketCount - 1) {
                DefaultJdbcStreamStateValue.cursorIncrementalCheckpoint(cursor, cursorLowerBound)
            } else {
                parent.completeState
            }

    override fun samplingQuery(sampleRateInvPow2: Int): SelectQuery =
        parent.samplingQuery(sampleRateInvPow2)
}

/**
 * Reads a partition as a sequence of bounded queries within one reader, checkpointing only once
 * the last query has been fully consumed. Used for large snapshots of any shape.
 *
 * [JdbcPartitionReader] is sealed, so this implements [PartitionReader] directly, reproducing
 * the same resource-acquisition and output-routing plumbing.
 */
class SnowflakeMultiQueryPartitionReader(
    val jdbcPartition: JdbcPartition<*>,
    val queries: List<SelectQuery>,
) : PartitionReader {

    private val log = KotlinLogging.logger {}

    val streamState: JdbcStreamState<*> = jdbcPartition.streamState
    val stream: Stream = streamState.stream
    val sharedState: JdbcSharedState = streamState.sharedState
    val selectQuerier: SelectQuerier = sharedState.selectQuerier

    val runComplete = AtomicBoolean(false)
    val numRecords = AtomicLong()

    private val partitionId: String = UUID.randomUUID().toString().replace("-", "").take(8)
    private val acquiredResources =
        AtomicReference<Map<ResourceType, JdbcPartitionReader.AcquiredResource>>()
    private lateinit var outputMessageRouter: OutputMessageRouter
    private lateinit var outputRoute:
        (
            MutableMap<String, FieldValueEncoder<*>>,
            Map<io.airbyte.cdk.discover.Field, FieldValueChange>?,
        ) -> Unit

    override fun tryAcquireResources(): PartitionReader.TryAcquireResourcesStatus {
        val resourceTypes: List<ResourceType> =
            when (streamState.streamFeedBootstrap.dataChannelMedium) {
                DataChannelMedium.STDIO -> listOf(ResourceType.RESOURCE_DB_CONNECTION)
                DataChannelMedium.SOCKET ->
                    listOf(
                        ResourceType.RESOURCE_DB_CONNECTION,
                        ResourceType.RESOURCE_OUTPUT_SOCKET,
                    )
            }
        val resources: Map<ResourceType, JdbcPartitionReader.AcquiredResource> =
            jdbcPartition.tryAcquireResourcesForReader(resourceTypes)
                ?: return PartitionReader.TryAcquireResourcesStatus.RETRY_LATER
        acquiredResources.set(resources)
        outputMessageRouter =
            OutputMessageRouter(
                streamState.streamFeedBootstrap.dataChannelMedium,
                streamState.streamFeedBootstrap.dataChannelFormat,
                streamState.streamFeedBootstrap.outputConsumer,
                mapOf("partition_id" to partitionId),
                streamState.streamFeedBootstrap,
                resources
                    .filter { it.value.resource != null }
                    .map { it.key to it.value.resource!! }
                    .toMap(),
            )
        outputRoute = outputMessageRouter.recordAcceptors[stream.id]!!
        return PartitionReader.TryAcquireResourcesStatus.READY_TO_RUN
    }

    override suspend fun run() {
        for ((index: Int, query: SelectQuery) in queries.withIndex()) {
            log.info {
                "Reading query ${index + 1} / ${queries.size} of partition " +
                    "for '${stream.label}'."
            }
            selectQuerier
                .executeQuery(query, SelectQuerier.Parameters(true, streamState.fetchSizeOrDefault))
                .use { result: SelectQuerier.Result ->
                    for (row in result) {
                        outputRoute(row.data, row.changes)
                        numRecords.incrementAndGet()
                    }
                }
        }
        runComplete.set(true)
    }

    override fun checkpoint(): PartitionReadCheckpoint {
        if (!runComplete.get()) {
            throw RuntimeException("cannot checkpoint partially read multi-query partition")
        }
        val checkpointPartitionId: String? =
            when (streamState.streamFeedBootstrap.dataChannelMedium) {
                DataChannelMedium.SOCKET -> partitionId
                DataChannelMedium.STDIO -> null
            }
        return PartitionReadCheckpoint(
            jdbcPartition.completeState,
            numRecords.get(),
            checkpointPartitionId,
        )
    }

    override fun releaseResources() {
        if (::outputMessageRouter.isInitialized) {
            outputMessageRouter.close()
        }
        acquiredResources.getAndSet(null)?.forEach { it.value.close() }
    }
}

/**
 * [io.airbyte.cdk.read.PartitionsCreator] which detects large single-query reads and subdivides
 * them with hash-bucket predicates. Everything else delegates to the stock
 * [JdbcConcurrentPartitionsCreator].
 */
class SnowflakeHashBucketPartitionsCreator(
    partition: DefaultJdbcPartition,
    partitionFactory:
        JdbcPartitionFactory<DefaultJdbcSharedState, DefaultJdbcStreamState, DefaultJdbcPartition>,
) :
    JdbcPartitionsCreator<DefaultJdbcSharedState, DefaultJdbcStreamState, DefaultJdbcPartition>(
        partition,
        partitionFactory,
    ) {

    private val log = KotlinLogging.logger {}

    private val delegate:
        JdbcConcurrentPartitionsCreator<
            DefaultJdbcSharedState,
            DefaultJdbcStreamState,
            DefaultJdbcPartition,
        > =
        JdbcConcurrentPartitionsCreator(partition, partitionFactory)

    override suspend fun run(): List<PartitionReader> {
        val p: DefaultJdbcPartition = partition
        return when (p) {
            is DefaultUnsplittableJdbcCursorIncrementalPartition ->
                runCursorIncremental(p, p.cursor, p.cursorLowerBound)
            is DefaultJdbcCursorIncrementalPartition ->
                runCursorIncremental(p, p.cursor, p.cursorLowerBound)
            // All snapshot shapes, splittable or not: stock splitting can silently produce
            // zero boundaries and fall back to one unbounded query, so take control of all
            // large snapshots here.
            else -> runSnapshot(p)
        }
    }

    /** Large cursor windows fan out into parallel bucket readers. */
    private suspend fun runCursorIncremental(
        incremental: JdbcCursorPartition<DefaultJdbcStreamState>,
        cursor: EmittedField,
        cursorLowerBound: JsonNode,
    ): List<PartitionReader> {
        ensureCursorUpperBound()
        if (streamState.cursorUpperBound == null || streamState.cursorUpperBound?.isNull == true) {
            log.info { "Maximum cursor column value query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        if (!sharedState.withSampling) {
            return delegate.run()
        }
        val sample: Sample<Long> = collectSample { row: SelectQuerier.ResultRow ->
            sharedState.rowByteSizeEstimator().apply(row.data.toJson())
        }
        if (sample.kind == Sample.Kind.EMPTY) {
            log.info { "Sampling query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        val expectedByteSize: Long = estimateAndSetFetchSize(sample)
        if (expectedByteSize <= SAFE_QUERY_BYTES) {
            return delegate.run()
        }
        val bucketCount: Int = bucketCountFor(expectedByteSize)
        log.info {
            "Large cursor window: will be read by $bucketCount concurrent " +
                "hash-bucketed partition readers."
        }
        return (0 until bucketCount).map { bucketIndex: Int ->
            JdbcNonResumablePartitionReader(
                SnowflakeHashBucketPartition(
                    incremental,
                    cursor,
                    cursorLowerBound,
                    bucketIndex,
                    bucketCount,
                )
            )
        }
    }

    /** Large snapshots of any shape are read as one reader running bounded queries sequentially. */
    private suspend fun runSnapshot(p: DefaultJdbcPartition): List<PartitionReader> {
        if (p is JdbcCursorPartition<*>) {
            ensureCursorUpperBound()
            if (
                streamState.cursorUpperBound == null || streamState.cursorUpperBound?.isNull == true
            ) {
                log.info { "Maximum cursor column value query found that the table was empty." }
                return listOf(CheckpointOnlyPartitionReader())
            }
        }
        if (!sharedState.withSampling) {
            return delegate.run()
        }
        val sample: Sample<Long> = collectSample { row: SelectQuerier.ResultRow ->
            sharedState.rowByteSizeEstimator().apply(row.data.toJson())
        }
        if (sample.kind == Sample.Kind.EMPTY) {
            log.info { "Sampling query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        val expectedByteSize: Long = estimateAndSetFetchSize(sample)
        if (expectedByteSize <= SAFE_QUERY_BYTES) {
            return delegate.run()
        }
        val bucketCount: Int = bucketCountFor(expectedByteSize)
        log.info {
            "Large snapshot: will be read by one reader running " +
                "$bucketCount sequential hash-bucketed queries."
        }
        val queries: List<SelectQuery> =
            (0 until bucketCount).map { bucketIndex: Int ->
                hashBucketQuery(p.nonResumableQuery, bucketIndex, bucketCount)
            }
        return listOf(SnowflakeMultiQueryPartitionReader(p, queries))
    }

    private fun estimateAndSetFetchSize(rowByteSizeSample: Sample<Long>): Long {
        streamState.fetchSize = sharedState.jdbcFetchSizeEstimator().apply(rowByteSizeSample)
        val expectedByteSize: Long =
            rowByteSizeSample.sampledValues.sum() * rowByteSizeSample.valueWeight
        log.info { "Partition size estimated at ${expectedByteSize shr 20} MiB." }
        return expectedByteSize
    }

    private fun bucketCountFor(expectedByteSize: Long): Int = hashBucketCount(expectedByteSize)

    companion object {
        const val MAX_BUCKET_COUNT = 512

        /**
         * Maximum estimated bytes for a single query's result set. Snowflake's staged result
         * chunks carry credentials with a finite lifetime (~6h observed) which the JDBC driver
         * never refreshes; a result set must be fully consumed within that lifetime, and the
         * consumption rate is dictated by the slowest stage of the pipeline (often the
         * destination). 512 MiB stays inside the lifetime for consumption rates down to
         * ~25 KB/s, observed as realistic for batched destinations.
         */
        const val SAFE_QUERY_BYTES: Long = 512L shl 20 // 512 MiB
    }
}

/** Factory shadowing the CDK's `@Secondary` concurrent factory for this connector. */
@Singleton
@Requires(property = MODE_PROPERTY, value = "concurrent")
class SnowflakeHashBucketPartitionsCreatorFactory(
    partitionFactory:
        JdbcPartitionFactory<DefaultJdbcSharedState, DefaultJdbcStreamState, DefaultJdbcPartition>,
) :
    JdbcPartitionsCreatorFactory<
        DefaultJdbcSharedState,
        DefaultJdbcStreamState,
        DefaultJdbcPartition,
    >(partitionFactory) {

    override fun partitionsCreator(
        partition: DefaultJdbcPartition
    ): JdbcPartitionsCreator<DefaultJdbcSharedState, DefaultJdbcStreamState, DefaultJdbcPartition> =
        SnowflakeHashBucketPartitionsCreator(partition, partitionFactory)
}
