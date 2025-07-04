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

package io.qalipsis.plugins.mongodb.save

import com.mongodb.client.result.InsertManyResult
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
import kotlinx.coroutines.runBlocking
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
    private val clientBuilder: () -> MongoClient,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?
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
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            recordsCounter = counter(scenarioName, stepName, "$meterPrefix-saving-records", tags).report {
                display(
                    format = "attempted req: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToResponse = timer(scenarioName, stepName, "$meterPrefix-time-to-response", tags)
            successCounter = counter(scenarioName, stepName, "$meterPrefix-successes", tags).report {
                display(
                    format = "\u2713 %,.0f req",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
            failureCounter = counter(scenarioName, stepName, "$meterPrefix-failures", tags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 4,
                    Counter::count
                )
            }
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

                    runBlocking {
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
                    runBlocking {
                        result.set(Result.success(MongoDbSaveQueryMeters(saved.get(), failed, duration)))
                    }
                }
            })
        return result.get().getOrThrow()
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
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
