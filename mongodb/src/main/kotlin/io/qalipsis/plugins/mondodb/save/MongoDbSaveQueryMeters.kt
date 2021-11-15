package io.qalipsis.plugins.mondodb.save

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property savedRecords count of saved records
 * @property savedBytes total bytes saved
 * @property timeToResult time to until the confirmation of the successful response
 *
 * @author Eric Jessé
 */
data class MongoDbSaveQueryMeters(
    val savedRecords: Int,
    val failedRecords: Int,
    val timeToResult: Duration
)
