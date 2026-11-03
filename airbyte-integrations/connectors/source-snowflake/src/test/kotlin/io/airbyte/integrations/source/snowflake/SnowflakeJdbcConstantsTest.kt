/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake

import io.airbyte.cdk.jdbc.DefaultJdbcConstants
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/** The JDBC read settings in `application.yml`. */
@MicronautTest
class SnowflakeJdbcConstantsTest {

    @Inject lateinit var constants: DefaultJdbcConstants

    @Test
    fun testLargeTableSplits() {
        Assertions.assertTrue(constants.withSampling)
        Assertions.assertTrue(constants.withLargeTableSplits)
    }
}
