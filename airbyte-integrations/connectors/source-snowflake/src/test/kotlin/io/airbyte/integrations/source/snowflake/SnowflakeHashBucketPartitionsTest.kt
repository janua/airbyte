/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake

import io.airbyte.cdk.discover.EmittedField
import io.airbyte.cdk.jdbc.StringFieldType
import io.airbyte.cdk.read.SelectQuery
import io.airbyte.cdk.util.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class SnowflakeHashBucketPartitionsTest {

    private val columns =
        listOf(
            EmittedField("VXID", StringFieldType),
            EmittedField("PERMUTIVE_ID", StringFieldType),
        )

    @Test
    fun `hashBucketQuery adds a WHERE clause when the query has none`() {
        val q = SelectQuery("""SELECT "VXID", "PERMUTIVE_ID" FROM "S"."T"""", columns, emptyList())
        val bucketed = hashBucketQuery(q, hasWhere = false, bucketIndex = 3, bucketCount = 8)
        assertEquals(
            """SELECT "VXID", "PERMUTIVE_ID" FROM "S"."T" WHERE MOD(ABS(HASH("VXID", "PERMUTIVE_ID")), 8) = 3""",
            bucketed.sql,
        )
    }

    @Test
    fun `hashBucketQuery appends with AND when the query already has a WHERE clause`() {
        val q =
            SelectQuery(
                """SELECT "VXID", "PERMUTIVE_ID" FROM "S"."T" WHERE ((("CREATED_AT" >= ?))) AND ((("CREATED_AT" <= ?)))""",
                columns,
                emptyList(),
            )
        val bucketed = hashBucketQuery(q, hasWhere = true, bucketIndex = 0, bucketCount = 2)
        assertEquals(
            """SELECT "VXID", "PERMUTIVE_ID" FROM "S"."T" WHERE ((("CREATED_AT" >= ?))) AND ((("CREATED_AT" <= ?))) AND MOD(ABS(HASH("VXID", "PERMUTIVE_ID")), 2) = 0""",
            bucketed.sql,
        )
    }

    @Test
    fun `hashBucketQuery preserves columns and bindings`() {
        val bindings = listOf(SelectQuery.Binding(Jsons.textNode("2026-07-13T00:00:00"), StringFieldType))
        val q = SelectQuery("""SELECT "VXID", "PERMUTIVE_ID" FROM "S"."T" WHERE "X" >= ?""", columns, bindings)
        val bucketed = hashBucketQuery(q, hasWhere = true, bucketIndex = 1, bucketCount = 4)
        assertSame(columns, bucketed.columns)
        assertSame(bindings, bucketed.bindings)
    }

    @Test
    fun `hashBucketQuery does not depend on keyword casing in the rendered SQL`() {
        val q = SelectQuery("""select "VXID", "PERMUTIVE_ID" from "S"."T" where "X" >= ?""", columns, emptyList())
        val bucketed = hashBucketQuery(q, hasWhere = true, bucketIndex = 1, bucketCount = 2)
        assertEquals(
            """select "VXID", "PERMUTIVE_ID" from "S"."T" where "X" >= ? AND MOD(ABS(HASH("VXID", "PERMUTIVE_ID")), 2) = 1""",
            bucketed.sql,
        )
    }

    @Test
    fun `hashBucketQuery is not misled by identifiers containing the word WHERE`() {
        val q = SelectQuery("""SELECT "VXID", "PERMUTIVE_ID" FROM "S"."MY WHERE TABLE"""", columns, emptyList())
        val bucketed = hashBucketQuery(q, hasWhere = false, bucketIndex = 0, bucketCount = 2)
        assertEquals(
            """SELECT "VXID", "PERMUTIVE_ID" FROM "S"."MY WHERE TABLE" WHERE MOD(ABS(HASH("VXID", "PERMUTIVE_ID")), 2) = 0""",
            bucketed.sql,
        )
    }

    @Test
    fun `hashBucketQuery rejects a query with no columns to hash`() {
        val q = SelectQuery("""SELECT 1 FROM "S"."T"""", emptyList(), emptyList())
        assertThrows(IllegalArgumentException::class.java) { hashBucketQuery(q, false, 0, 2) }
    }

    @Test
    fun `hashBucketCount sizes buckets to the safe query size with a floor of two and a cap`() {
        val safe = SnowflakeHashBucketPartitionsCreator.SAFE_QUERY_BYTES
        val cap = SnowflakeHashBucketPartitionsCreator.MAX_BUCKET_COUNT
        assertEquals(2, hashBucketCount(1L))
        assertEquals(2, hashBucketCount(safe))
        assertEquals(2, hashBucketCount(safe + 1))
        assertEquals(3, hashBucketCount(2 * safe + 1))
        assertEquals(18, hashBucketCount(8768L shl 20)) // the 300M-row validation table
        assertEquals(cap, hashBucketCount(Long.MAX_VALUE / 4))
    }
}
