/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.mongodb.save

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
