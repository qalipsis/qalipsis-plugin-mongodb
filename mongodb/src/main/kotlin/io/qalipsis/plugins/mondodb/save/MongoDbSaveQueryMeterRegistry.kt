package io.qalipsis.plugins.mondodb.save

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

/**
 * Wrapper for the meters of the MongoDB save operations.
 *
 * @author Alexander Sosnovsky
 */
data class MongoDbSaveQueryMeterRegistry(
    val recordsCount: Counter?,
    val timeToResponse: Timer?,
    val successCounter: Counter?,
    val failureCounter: Counter?
) {

    /**
     * Records the number of produced records.
     */
    fun countRecords(size: Int) = recordsCount?.increment(size.toDouble())

    /**
     * Records the time to response in nanoseconds.
     */
    fun recordTimeToResponse(durationNanos: Long) = timeToResponse?.record(durationNanos, TimeUnit.NANOSECONDS)

    /**
     * Records a new success.
     */
    fun countSuccess(size: Int) = successCounter?.increment(size.toDouble())

    /**
     * Records a new failure.
     */
    fun countFailure(size: Int) = failureCounter?.increment(size.toDouble())
}
