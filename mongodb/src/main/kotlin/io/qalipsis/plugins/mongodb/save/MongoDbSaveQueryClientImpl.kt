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

import com.mongodb.client.result.InsertManyResult
import com.mongodb.reactivestreams.client.MongoClient
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.Slot
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.bson.Document
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger


/**
 * Implementation of [MongoDbSaveQueryClient].
 * Client to query to MongoDB.
 *
 * @property clientBuilder supplier for the MongoDb client.
 * @property eventsLogger the logger for events to track what happens during save query execution.
 * @property meterRegistry registry for the meters.
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSaveQueryClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientBuilder: () -> MongoClient,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
) : MongoDbSaveQueryClient {

    private lateinit var client: MongoClient

    private val eventPrefix = "mongodb.save"

    private val meterPrefix = "mongodb-save"

    private var recordsCounter: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        client = clientBuilder()
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            recordsCounter = counter("$meterPrefix-saving-records", tags)
            timeToResponse = timer("$meterPrefix-time-to-response", tags)
            successCounter = counter("$meterPrefix-successes", tags)
            failureCounter = counter("$meterPrefix-failures", tags)
        }
    }

    override suspend fun execute(
        dbName: String,
        collName: String,
        records: List<Document>,
        contextEventTags: Map<String, String>
    ): MongoDbSaveQueryMeters {
        val result = Slot<Result<MongoDbSaveQueryMeters>>()
        val isFirst = AtomicBoolean(true)

        eventsLogger?.debug("$eventPrefix.saving-records", records.size, tags = contextEventTags)
        val requestStart = System.nanoTime()
        val saved = AtomicInteger()
        client.getDatabase(dbName)
            .getCollection(collName)
            .insertMany(records)
            .subscribe(object : Subscriber<InsertManyResult> {
                override fun onSubscribe(s: Subscription) {
                    s.request(Long.MAX_VALUE)
                }

                override fun onNext(result: InsertManyResult) {
                    // flag isFirst is only use to know what is the first save document.
                    if (isFirst.get()) {
                        val timeDuration = System.nanoTime() - requestStart
                        eventsLogger?.info(
                            "$eventPrefix.time-to-response",
                            Duration.ofNanos(timeDuration),
                            tags = contextEventTags
                        )
                        timeToResponse?.record(timeDuration, TimeUnit.NANOSECONDS)
                        isFirst.set(false)
                    }
                    saved.addAndGet(result.insertedIds.size)
                }

                override fun onError(error: Throwable) {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.warn("$eventPrefix.failure", arrayOf(error, duration), tags = contextEventTags)
                    failureCounter?.increment(records.size.toDouble())

                    ioCoroutineScope.launch {
                        result.set(Result.failure(error))
                    }
                }

                override fun onComplete() {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.info(
                        "$eventPrefix.saved-records",
                        arrayOf(duration, saved),
                        tags = contextEventTags
                    )
                    val failed = records.size - saved.get()
                    recordsCounter?.increment(saved.get().toDouble())
                    if (failed > 0) {
                        failureCounter?.increment(failed.toDouble())
                        eventsLogger?.warn(
                            "$eventPrefix.failed-records",
                            failed,
                            tags = contextEventTags
                        )
                    }
                    ioCoroutineScope.launch {
                        result.set(Result.success(MongoDbSaveQueryMeters(saved.get(), failed, duration)))
                    }
                }
            })
        return result.get().getOrThrow()
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(recordsCounter!!)
            remove(timeToResponse!!)
            remove(successCounter!!)
            remove(failureCounter!!)
            recordsCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
        tryAndLog(log) {
            client.close()
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
