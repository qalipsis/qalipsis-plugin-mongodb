/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.mongodb.search

import com.mongodb.reactivestreams.client.MongoClient
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.sync.Slot
import io.qalipsis.plugins.mongodb.Sorting
import kotlinx.coroutines.runBlocking
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
    private val clientFactory: () -> MongoClient,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?
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
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            recordsCount = counter(scenarioName, stepName, "$meterPrefix-received-records", tags).report {
                display(
                    format = "attempted req: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToResponse = timer(scenarioName, stepName, "$meterPrefix-time-to-response", tags)
            successCounter = counter(scenarioName, stepName, "$meterPrefix-success", tags).report {
                display(
                    format = "\u2713 %,.0f req",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            failureCounter = counter(scenarioName, stepName, "$meterPrefix-failure", tags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
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
    ): io.qalipsis.plugins.mongodb.MongoDBQueryResult {

        val results = mutableListOf<Document>()

        val latch = Slot<Result<io.qalipsis.plugins.mongodb.MongoDBQueryResult>>()
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

                    runBlocking {
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

                    runBlocking {
                        latch.set(
                            Result.success(
                                io.qalipsis.plugins.mongodb.MongoDBQueryResult(
                                    results,
                                    io.qalipsis.plugins.mongodb.MongoDbQueryMeters(results.size, duration)
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
            result.append(
                name, if (Sorting.ASC == order) {
                    1
                } else {
                    -1
                }
            )
        }
        return result
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}


