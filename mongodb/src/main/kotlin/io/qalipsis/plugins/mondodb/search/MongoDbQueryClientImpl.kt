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

package io.qalipsis.plugins.mondodb.search

import com.mongodb.reactivestreams.client.MongoClient
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.Slot
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import io.qalipsis.plugins.mondodb.Sorting
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.bson.Document
import org.bson.conversions.Bson
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Implementation of [MongoDbQueryClient].
 * Client to query from MongoDB.
 *
 * @property clientFactory supplier for the MongoDb client
 * @property eventsLogger the logger for events to track what happens during save query execution.
 * @property meterRegistry registry for the meters.
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbQueryClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientFactory: () -> MongoClient,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
) : MongoDbQueryClient {

    private lateinit var client: MongoClient

    private val eventPrefix = "mongodb.search"

    private val meterPrefix = "mongodb-search"

    private var recordsCount: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null


    /**
     * Prepare client inside before execute
     */
    override suspend fun init() {
        client = clientFactory()
    }

    override suspend fun start(context: StepStartStopContext) {
        init()
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            recordsCount = counter("$meterPrefix-received-records", tags)
            timeToResponse = timer("$meterPrefix-time-to-response", tags)
            successCounter = counter("$meterPrefix-success", tags)
            failureCounter = counter("$meterPrefix-failure", tags)
        }
    }

    /**
     * Executes a query and returns the list of results.
     */
    override suspend fun execute(
        database: String,
        collection: String,
        findClause: Bson,
        sorting: LinkedHashMap<String, Sorting>,
        contextEventTags: Map<String, String>
    ): MongoDBQueryResult {

        val results = mutableListOf<Document>()

        val latch = Slot<Result<MongoDBQueryResult>>()
        val isFirst = AtomicBoolean(true)

        eventsLogger?.debug("$eventPrefix.searching", tags = contextEventTags)
        val requestStart = System.nanoTime()
        client.getDatabase(database)
            .getCollection(collection)
            .find(findClause)
            .sort(sort(sorting))
            .subscribe(object : Subscriber<Document> {

                override fun onSubscribe(s: Subscription) {
                    s.request(Long.MAX_VALUE)
                }

                override fun onNext(document: Document) {
                    // flag isFirst is only use to know what is the first search document.
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
                    results.add(document)
                }

                override fun onError(error: Throwable) {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.warn("$eventPrefix.failure", arrayOf(error, duration), tags = contextEventTags)
                    failureCounter?.increment()

                    ioCoroutineScope.launch {
                        latch.set(Result.failure(error))
                    }
                }

                override fun onComplete() {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.info(
                        "$eventPrefix.success",
                        arrayOf(duration, results.size),
                        tags = contextEventTags
                    )
                    successCounter?.increment()
                    recordsCount?.increment(results.size.toDouble())

                    ioCoroutineScope.launch {
                        latch.set(
                            Result.success(
                                MongoDBQueryResult(
                                    results,
                                    MongoDbQueryMeters(results.size, duration)
                                )
                            )
                        )
                    }
                }
            })

        return latch.get().getOrThrow()
    }

    /**
     * Shutdown client after execute
     */
    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(recordsCount!!)
            remove(timeToResponse!!)
            remove(successCounter!!)
            remove(failureCounter!!)
            recordsCount = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
        tryAndLog(log) {
            client.close()
        }
    }

    private fun sort(sorting: LinkedHashMap<String, Sorting>): Bson {
        val result = Document()
        sorting.forEach { (name, order) ->
            if (Sorting.ASC.equals(order)) {
                result.append(name, 1)
            } else if (Sorting.DESC.equals(order)) {
                result.append(name, -1)
            }
        }
        return result
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}


