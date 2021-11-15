package io.qalipsis.plugins.mondodb.search

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit

/**
 * Wrapper for the meters of the MongoDB search operations.
 *
 * @author Alexander Sosnovsky
 */
internal data class MongoDbQueryMeterRegistry(
    val recordsCount: Counter?,
    val timeToResponse: Timer?,
    val successCounter: Counter?,
    val failureCounter: Counter?,
) {

    /**
     * Records the number of received records.
     */
    fun countRecords(size: Int) = recordsCount?.increment(size.toDouble())

    /**
     * Records the time to response in nanoseconds.
     */
    fun recordTimeToResponse(durationNanos: Long) = timeToResponse?.record(durationNanos, TimeUnit.NANOSECONDS)

    /**
     * Records a new success.
     */
    fun countSuccess() = successCounter?.increment()

    /**
     * Records a new failure.
     */
    fun countFailure() = failureCounter?.increment()
}
