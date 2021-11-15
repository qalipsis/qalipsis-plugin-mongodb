package io.qalipsis.plugins.mondodb

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property fetchedRecords count of received records
 * @property fetchedBytes total count of received bytes
 * @property timeToResult time to until the complete successful response
 *
 * @author Eric Jessé
 */
data class MongoDbQueryMeters(
    val fetchedRecords: Int,
    val timeToResult: Duration
)
