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

package io.qalipsis.plugins.mongodb.poll

import com.mongodb.reactivestreams.client.MongoClient
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
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
 * @property clientFactory supplier for the MongoDb client
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
    private val meterRegistry: MeterRegistry?
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
            val tags = context.toMetersTags()
            recordsCount = counter("$meterPrefix-received-records", tags)
            timeToResponse = timer("$meterPrefix-time-to-response", tags)
            successCounter = counter("$meterPrefix-successes", tags)
            failureCounter = counter("$meterPrefix-failures", tags)
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
            remove(recordsCount!!)
            remove(timeToResponse!!)
            remove(successCounter!!)
            remove(failureCounter!!)
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
