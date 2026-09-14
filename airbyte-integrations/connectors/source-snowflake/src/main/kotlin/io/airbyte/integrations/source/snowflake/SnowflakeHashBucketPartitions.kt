/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.TransientErrorException
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.discover.DataField
import io.airbyte.cdk.discover.DataOrMetaField
import io.airbyte.cdk.discover.EmittedField
import io.airbyte.cdk.discover.FieldType
import io.airbyte.cdk.jdbc.IntFieldType
import io.airbyte.cdk.jdbc.LongFieldType
import io.airbyte.cdk.output.DataChannelMedium
import io.airbyte.cdk.output.OutputMessageRouter
import io.airbyte.cdk.output.sockets.FieldValueEncoder
import io.airbyte.cdk.output.sockets.toJson
import io.airbyte.cdk.read.And
import io.airbyte.cdk.read.DefaultJdbcCursorIncrementalPartition
import io.airbyte.cdk.read.DefaultJdbcPartition
import io.airbyte.cdk.read.DefaultJdbcSharedState
import io.airbyte.cdk.read.DefaultJdbcSplittablePartition
import io.airbyte.cdk.read.DefaultJdbcStreamState
import io.airbyte.cdk.read.DefaultJdbcStreamStateValue
import io.airbyte.cdk.read.DefaultJdbcUnsplittablePartition
import io.airbyte.cdk.read.DefaultUnsplittableJdbcCursorIncrementalPartition
import io.airbyte.cdk.read.Equal
import io.airbyte.cdk.read.FieldValueChange
import io.airbyte.cdk.read.JdbcConcurrentPartitionsCreator
import io.airbyte.cdk.read.JdbcCursorPartition
import io.airbyte.cdk.read.JdbcNonResumablePartitionReader
import io.airbyte.cdk.read.JdbcPartition
import io.airbyte.cdk.read.JdbcPartitionFactory
import io.airbyte.cdk.read.JdbcPartitionReader
import io.airbyte.cdk.read.JdbcPartitionsCreator
import io.airbyte.cdk.read.JdbcPartitionsCreatorFactory
import io.airbyte.cdk.read.JdbcSharedState
import io.airbyte.cdk.read.JdbcStreamState
import io.airbyte.cdk.read.MODE_PROPERTY
import io.airbyte.cdk.read.NoWhere
import io.airbyte.cdk.read.PartitionReadCheckpoint
import io.airbyte.cdk.read.PartitionReader
import io.airbyte.cdk.read.ResourceType
import io.airbyte.cdk.read.Sample
import io.airbyte.cdk.read.SelectColumns
import io.airbyte.cdk.read.SelectQuerier
import io.airbyte.cdk.read.SelectQuery
import io.airbyte.cdk.read.SelectQuerySpec
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.Where
import io.airbyte.cdk.read.WhereClauseNode
import io.airbyte.cdk.read.WhereNode
import io.airbyte.cdk.read.optimize
import io.airbyte.cdk.util.Jsons
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/*
 * Large single-query reads.
 *
 * Snowflake stages large query results as chunks whose download credentials expire (~6h observed)
 * and are never refreshed by the JDBC driver, so a result set that takes longer than that to consume
 * fails with `Max retry reached for the download of chunk#N ... HTTP status=403` and never makes
 * progress. This connector subdivides any read estimated above [SAFE_QUERY_BYTES], whatever its
 * partition shape, with `MOD(ABS(HASH(col, ...)), n) = ?` predicates into n disjoint,
 * jointly-complete queries small enough to be consumed within that lifetime. Cursor-incremental
 * windows fan out into parallel bucket readers; snapshots run their buckets sequentially in one
 * reader with a single end-of-partition checkpoint.
 */

/**
 * A SQL expression standing in for a column in the CDK's query AST. The AST node types are sealed,
 * so a connector cannot add nodes of its own; a [DataField] is the one open extension point, and
 * [SnowflakeSourceOperations] renders these verbatim instead of quoting them as identifiers.
 */
sealed interface SnowflakeSqlExpression : DataField

/** `COUNT(*)`, used to size a large read exactly when the row sample has saturated. */
data object SnowflakeRowCountColumn : SnowflakeSqlExpression {
    override val id: String = "COUNT(*)"
    override val type: FieldType = LongFieldType
}

/**
 * The bucketing predicate's left-hand side, `MOD(ABS(HASH(col, ...)), bucketCount)`. Compares as an
 * integer, so the bucket index binds as a parameter. Hashes an explicit column list because
 * `HASH(*)` is only legal in a SELECT clause in Snowflake.
 */
data class SnowflakeHashBucketColumn(
    val hashedColumns: List<DataField>,
    val bucketCount: Int,
) : SnowflakeSqlExpression {
    init {
        require(hashedColumns.isNotEmpty()) { "cannot hash-bucket a query with no columns" }
    }
    override val id: String =
        "MOD(ABS(HASH(${hashedColumns.joinToString(", ") { "\"${it.id}\"" }})), $bucketCount)"
    override val type: FieldType = IntFieldType
}

/**
 * Columns whose hash assigns a row to a bucket. Any deterministic subset keeps the buckets disjoint
 * and complete; the choice only affects balance and stability. A primary key gives both, so it is
 * preferred. Without one, the cursor is left out because cursor columns are the ones most often
 * derived (e.g. `DATE(MAX(...))` in a view), and a value that changes between bucket queries would
 * move rows between buckets already read and buckets still to come.
 */
internal fun hashColumnsFor(
    configuredPrimaryKey: List<EmittedField>?,
    configuredCursor: DataOrMetaField?,
    projected: List<DataField>,
): List<DataField> {
    if (!configuredPrimaryKey.isNullOrEmpty()) return configuredPrimaryKey
    val stable: List<DataField> = projected.filter { it.id != configuredCursor?.id }
    return stable.ifEmpty { projected }
}

/** The projected columns of [this], which must be a plain column projection. */
internal val SelectQuerySpec.projectedColumns: List<DataField>
    get() = (select as? SelectColumns)?.columns ?: error("expected a column projection")

/** Restricts [this] to hash bucket [bucketIndex] of [bucketCount] by extending its WHERE clause. */
internal fun SelectQuerySpec.withHashBucket(
    hashColumns: List<DataField>,
    bucketIndex: Int,
    bucketCount: Int,
): SelectQuerySpec {
    val leaf: WhereClauseNode =
        Equal(SnowflakeHashBucketColumn(hashColumns, bucketCount), Jsons.numberNode(bucketIndex))
    val clause: WhereClauseNode =
        when (val w: WhereNode = where) {
            NoWhere -> leaf
            is Where -> And(w.clause, leaf)
        }
    return copy(where = Where(clause))
}

/** The non-resumable query spec of a default partition, regardless of splittability. */
internal fun DefaultJdbcPartition.nonResumableSpec(): SelectQuerySpec =
    when (this) {
        is DefaultJdbcUnsplittablePartition -> nonResumableQuerySpec
        is DefaultJdbcSplittablePartition -> nonResumableQuerySpec
        else -> error("unexpected partition type ${this::class.qualifiedName}")
    }

/** Generates the bucketed non-resumable query for [this] through its own query generator. */
internal fun DefaultJdbcPartition.hashBucketQuery(bucketIndex: Int, bucketCount: Int): SelectQuery {
    val spec: SelectQuerySpec = nonResumableSpec()
    val stream: Stream = streamState.stream
    val hashColumns: List<DataField> =
        hashColumnsFor(stream.configuredPrimaryKey, stream.configuredCursor, spec.projectedColumns)
    return selectQueryGenerator.generate(
        spec.withHashBucket(hashColumns, bucketIndex, bucketCount).optimize()
    )
}

/** `SELECT COUNT(*)` over exactly the rows [this] partition's non-resumable query would read. */
internal fun SelectQuerySpec.asRowCount(): SelectQuerySpec =
    copy(select = SelectColumns(listOf(SnowflakeRowCountColumn)))

internal fun DefaultJdbcPartition.rowCountQuery(): SelectQuery =
    selectQueryGenerator.generate(nonResumableSpec().asRowCount().optimize())

/**
 * Bytes a read is expected to produce. The CDK sample weight is a floor once the sample saturates (
 * [Sample.Kind.LARGE]: 1024 rows at 1/65536 caps the estimate near 67M rows whatever the table
 * holds), so in that case the size comes from an exact row count times the mean sampled row size.
 */
internal fun estimateByteSize(sample: Sample<Long>, rowCount: Long?): Long {
    val fromSample: Long = sample.sampledValues.sum() * sample.valueWeight
    if (sample.kind != Sample.Kind.LARGE || rowCount == null || sample.sampledValues.isEmpty()) {
        return fromSample
    }
    val meanRowBytes: Long = sample.sampledValues.sum() / sample.sampledValues.size
    return maxOf(fromSample, rowCount * meanRowBytes)
}

/**
 * Number of buckets needed to keep each query's result set within
 * [SnowflakeHashBucketPartitionsCreator.SAFE_QUERY_BYTES].
 */
internal fun hashBucketCount(expectedByteSize: Long): Int {
    val safe: Long = SnowflakeHashBucketPartitionsCreator.SAFE_QUERY_BYTES
    return ((expectedByteSize + safe - 1) / safe)
        .coerceIn(2L, SnowflakeHashBucketPartitionsCreator.MAX_BUCKET_COUNT.toLong())
        .toInt()
}

/**
 * One of [bucketCount] hash buckets subdividing a cursor-incremental partition's window. Buckets
 * run in parallel and [io.airbyte.cdk.read.FeedReader] publishes checkpoints in list order, so
 * every bucket but the last reports the window's lower bound and only the last reports the upper
 * bound: a failure anywhere resumes from the original window (duplicates possible, data loss not).
 */
class SnowflakeHashBucketPartition(
    val parent: DefaultJdbcPartition,
    val cursor: EmittedField,
    val cursorLowerBound: JsonNode,
    val bucketIndex: Int,
    val bucketCount: Int,
) : JdbcPartition<DefaultJdbcStreamState> {

    override val streamState: DefaultJdbcStreamState = parent.streamState

    override val nonResumableQuery: SelectQuery
        get() = parent.hashBucketQuery(bucketIndex, bucketCount)

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
 * Reads a partition as a sequence of bounded queries within one reader, checkpointing only once the
 * last query has been fully consumed. Used for large snapshots of any shape.
 *
 * [JdbcPartitionReader] is sealed, so this implements [PartitionReader] directly, reproducing the
 * same resource-acquisition and output-routing plumbing.
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
    private var outputMessageRouter: OutputMessageRouter? = null
    private lateinit var outputRoute:
        (
            MutableMap<String, FieldValueEncoder<*>>,
            Map<EmittedField, FieldValueChange>?,
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
        val router =
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
        outputMessageRouter = router
        outputRoute = router.recordAcceptors[stream.id]!!
        return PartitionReader.TryAcquireResourcesStatus.READY_TO_RUN
    }

    override suspend fun run() {
        outputPendingMessages()
        checkMaxReadTimeElapsed()
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

    /** Mirrors [JdbcPartitionReader]: drain state and status messages queued for SOCKET output. */
    private fun outputPendingMessages() {
        if (streamState.streamFeedBootstrap.dataChannelMedium == DataChannelMedium.STDIO) return
        val router: OutputMessageRouter = outputMessageRouter ?: return
        while (true) {
            when (val pending: Any = PartitionReader.pendingStates.poll() ?: break) {
                is AirbyteStateMessage -> router.acceptNonRecord(pending)
                is AirbyteStreamStatusTraceMessage -> router.acceptNonRecord(pending)
            }
        }
    }

    /**
     * Mirrors [JdbcPartitionReader]: refuse to start once the configured snapshot budget is spent.
     */
    private fun checkMaxReadTimeElapsed() {
        val max: Duration = sharedState.configuration.maxSnapshotReadDuration ?: return
        if (Duration.between(sharedState.snapshotReadStartTime, Instant.now()) > max) {
            throw TransientErrorException("Shutting down snapshot reader: max duration elapsed")
        }
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
        outputMessageRouter?.close()
        outputMessageRouter = null
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

    /** The one sample this creator takes; the delegate reuses it instead of sampling again. */
    private var cachedSample: Sample<SelectQuerier.ResultRow>? = null

    private val delegate:
        JdbcConcurrentPartitionsCreator<
            DefaultJdbcSharedState,
            DefaultJdbcStreamState,
            DefaultJdbcPartition,
        > =
        object :
            JdbcConcurrentPartitionsCreator<
                DefaultJdbcSharedState,
                DefaultJdbcStreamState,
                DefaultJdbcPartition,
            >(partition, partitionFactory) {
            override fun <T> collectSample(
                recordMapper: (SelectQuerier.ResultRow) -> T,
            ): Sample<T> = cachedSample?.map(recordMapper) ?: super.collectSample(recordMapper)
        }

    private fun sampleRows(): Sample<SelectQuerier.ResultRow> =
        collectSample { row: SelectQuerier.ResultRow -> row }.also { cachedSample = it }

    private fun rowByteSizes(sample: Sample<SelectQuerier.ResultRow>): Sample<Long> =
        sample.map { row: SelectQuerier.ResultRow ->
            sharedState.rowByteSizeEstimator().apply(row.data.toJson())
        }

    private fun countRows(p: DefaultJdbcPartition): Long? {
        log.info { "Row sample saturated; counting rows to size the read." }
        val record =
            selectQuerier.executeQuery(p.rowCountQuery()).use {
                if (it.hasNext()) it.next().data.toJson() else null
            }
        val count: Long? = record?.fields()?.asSequence()?.firstOrNull()?.value?.asLong()
        log.info { "Row count is ${count ?: "unknown"}." }
        return count
    }

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
        incremental: DefaultJdbcPartition,
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
        val sample: Sample<Long> = rowByteSizes(sampleRows())
        if (sample.kind == Sample.Kind.EMPTY) {
            log.info { "Sampling query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        val expectedByteSize: Long = estimateAndSetFetchSize(incremental, sample)
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
        val sample: Sample<Long> = rowByteSizes(sampleRows())
        if (sample.kind == Sample.Kind.EMPTY) {
            log.info { "Sampling query found that the table was empty." }
            return listOf(CheckpointOnlyPartitionReader())
        }
        val expectedByteSize: Long = estimateAndSetFetchSize(p, sample)
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
                p.hashBucketQuery(bucketIndex, bucketCount)
            }
        return listOf(SnowflakeMultiQueryPartitionReader(p, queries))
    }

    private fun estimateAndSetFetchSize(p: DefaultJdbcPartition, sample: Sample<Long>): Long {
        streamState.fetchSize = sharedState.jdbcFetchSizeEstimator().apply(sample)
        val rowCount: Long? = if (sample.kind == Sample.Kind.LARGE) countRows(p) else null
        val expectedByteSize: Long = estimateByteSize(sample, rowCount)
        log.info { "Partition size estimated at ${expectedByteSize shr 20} MiB." }
        return expectedByteSize
    }

    private fun bucketCountFor(expectedByteSize: Long): Int = hashBucketCount(expectedByteSize)

    companion object {
        const val MAX_BUCKET_COUNT = 512

        /**
         * Maximum estimated result-set bytes per query, sized so each query is consumed well within
         * the staged-chunk credential lifetime even at slow (~25 KB/s) destination rates.
         */
        const val SAFE_QUERY_BYTES: Long = 512L shl 20 // 512 MiB
    }
}

/** Replaces the CDK's `@Secondary` concurrent factory with one that hash-buckets large reads. */
@Singleton
@Primary
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
