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

package io.qalipsis.plugins.mongodb.poll

import com.mongodb.reactivestreams.client.MongoClient
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.Latch
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.plugins.mongodb.MongoDbQueryMeters
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Database reader based upon [MongoDB driver with reactive streams][https://mongodb.github.io/mongo-java-driver/].
 *
 * @property clientBuilder supplier for the MongoDb client
 * @property pollStatement statement to execute
 * @property pollDelay duration between the end of a poll and the start of the next one
 * @property resultsChannelFactory factory to create the channel containing the received results sets
 * @property running running state of the reader
 * @property pollingJob instance of the background job polling data from the database
 * @property eventsLogger the logger for events to track what happens during save query execution.
 * @property meterRegistry registry for the meters.
 *
 * @author Maxim Golokhov
 */
internal class MongoDbIterativeReader(
    private val coroutineScope: CoroutineScope,
    private val clientBuilder: () -> MongoClient,
    private val pollStatement: MongoDbPollStatement,
    private val pollDelay: Duration,
    private val resultsChannelFactory: () -> Channel<MongoDBQueryResult> = { Channel(Channel.UNLIMITED) },
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?
) : DatasourceIterativeReader<MongoDBQueryResult> {

    private val eventPrefix = "mongodb.poll"

    private val meterPrefix = "mongodb-poll"

    private var running = false

    private lateinit var client: MongoClient

    private lateinit var pollingJob: Job

    private lateinit var resultsChannel: Channel<MongoDBQueryResult>

    private lateinit var context: StepStartStopContext

    private var latestId: Any? = null

    private var recordsCount: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override fun start(context: StepStartStopContext) {
        log.debug { "Starting the step with the context $context" }
        meterRegistry?.apply {
            val metersTags = context.toMetersTags()
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            recordsCount = counter(scenarioName, stepName, "$meterPrefix-received-records", metersTags).report {
                display(
                    format = "attempted req: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            timeToResponse = timer(scenarioName, stepName, "$meterPrefix-time-to-response", metersTags)
            successCounter = counter(scenarioName, stepName, "$meterPrefix-successes", metersTags).report {
                display(
                    format = "\u2713 %,.0f req",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            failureCounter = counter(scenarioName, stepName, "$meterPrefix-failures", metersTags).report {
                display(
                    format = "\u2716 %,.0f failures",
                    severity = ReportMessageSeverity.ERROR,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
        }
        this.context = context
        init()
        running = true
        pollingJob = coroutineScope.launch {
            log.debug { "Polling job just started for context $context" }
            try {
                while (running) {
                    poll(client)
                    if (running) {
                        delay(pollDelay.toMillis())
                    }
                }
                log.debug { "Polling job just completed for context $context" }
            } finally {
                resultsChannel.cancel()
            }
        }
    }

    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            recordsCount = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
        running = false
        runCatching {
            runBlocking {
                pollingJob.cancelAndJoin()
            }
        }
        runCatching {
            client.close()
        }
        resultsChannel.cancel()
        pollStatement.reset()
        latestId = null
    }

    @KTestable
    private fun init() {
        client = clientBuilder()
        resultsChannel = resultsChannelFactory()
    }

    private suspend fun poll(client: MongoClient) {
        try {
            val latch = Latch(true)
            val results = mutableListOf<Document>()
            val isFirst = AtomicBoolean(true)

            eventsLogger?.trace("$eventPrefix.polling", tags = context.toEventTags())
            val requestStart = System.nanoTime()
            client.getDatabase(pollStatement.databaseName)
                .getCollection(pollStatement.collectionName)
                .find(pollStatement.filter.also { log.trace { "Searching documents with the filter: $it" } })
                .sort(pollStatement.sorting)
                .subscribe(object : Subscriber<Document> {
                    override fun onSubscribe(s: Subscription) {
                        s.request(Long.MAX_VALUE)
                    }

                    override fun onNext(document: Document) {
                        if (isFirst.get()) {
                            val timeDuration = Duration.ofNanos(System.nanoTime() - requestStart)
                            eventsLogger?.info(
                                "$eventPrefix.success",
                                arrayOf(results.size, timeDuration),
                                tags = context.toEventTags()
                            )
                            timeToResponse?.record(timeDuration)
                            isFirst.set(false)
                        }

                        // If a document is received that was already received, it is skipped, as well as all the previous ones.
                        if (latestId != null && document[DOCUMENT_ID_KEY] == latestId) {
                            results.clear()
                        } else {
                            results += document
                        }
                    }

                    override fun onError(error: Throwable) {
                        val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                        eventsLogger?.warn(
                            "$eventPrefix.failure",
                            arrayOf(error, duration),
                            tags = context.toEventTags()
                        )
                        failureCounter?.increment()

                        latch.cancel()
                    }

                    override fun onComplete() {
                        val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                        coroutineScope.launch {
                            eventsLogger?.info(
                                "$eventPrefix.successful-response",
                                arrayOf(duration, results.size),
                                tags = context.toEventTags()
                            )
                            successCounter?.increment()
                            recordsCount?.increment(results.size.toDouble())
                            if (results.isNotEmpty()) {
                                log.debug { "Received ${results.size} documents" }
                                resultsChannel.send(
                                    MongoDBQueryResult(
                                        documents = results,
                                        meters = MongoDbQueryMeters(results.size, duration)
                                    )
                                )
                                val latestDocument = results.last()
                                latestId = latestDocument[DOCUMENT_ID_KEY]
                                log.trace { "Latest received id: $latestId" }
                                pollStatement.saveTieBreakerValueForNextPoll(latestDocument)
                            } else {
                                log.debug { "No new document was received" }
                            }
                            latch.cancel()
                        }
                    }
                })
            latch.await()
        } catch (e: InterruptedException) {
            // The exception is ignored.
        } catch (e: CancellationException) {
            // The exception is ignored.
        } catch (e: Exception) {
            log.error(e) { e.message }
        }
    }

    override suspend fun hasNext(): Boolean = running

    override suspend fun next(): MongoDBQueryResult = resultsChannel.receive()

    private companion object {

        /**
         * Key of the value containing the ID of the [Document].
         */
        const val DOCUMENT_ID_KEY = "_id"

        @JvmStatic
        val log = logger()
    }
}
