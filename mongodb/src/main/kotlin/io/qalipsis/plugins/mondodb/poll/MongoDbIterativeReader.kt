package io.qalipsis.plugins.mondodb.poll

import com.mongodb.reactivestreams.client.MongoClient
import io.aerisconsulting.catadioptre.KTestable
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.Latch
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import io.qalipsis.plugins.mondodb.search.MongoDbQueryMeterRegistry
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
 *
 * @author Maxim Golokhov
 */
internal class MongoDbIterativeReader(
    private val coroutineScope: CoroutineScope,
    private val clientBuilder: () -> MongoClient,
    private val pollStatement: MongoDbPollStatement,
    private val pollDelay: Duration,
    private val resultsChannelFactory: () -> Channel<MongoDBQueryResult> = { Channel(Channel.UNLIMITED) },
    private var eventsLogger: EventsLogger?,
    private var mongoDbPollMeterRegistry: MongoDbQueryMeterRegistry?
) : DatasourceIterativeReader<MongoDBQueryResult> {

    private var running = false

    private lateinit var client: MongoClient

    private lateinit var pollingJob: Job

    private lateinit var resultsChannel: Channel<MongoDBQueryResult>

    private lateinit var context: StepStartStopContext

    private var latestId: Any? = null

    override fun start(context: StepStartStopContext) {
        log.debug { "Starting the step with the context $context" }
        this.context = context
        init()
        pollingJob = coroutineScope.launch {
            log.debug { "Polling job just started for context $context" }
            try {
                while (running) {
                    poll(client)
                    if (running)
                        delay(pollDelay.toMillis())
                }
                log.debug { "Polling job just completed for context $context" }
            } finally {
                resultsChannel.cancel()
            }
        }
        running = true
    }

    override fun stop(context: StepStartStopContext) {
        running = false
        runCatching {
            client.close()
        }
        runCatching {
            runBlocking {
                pollingJob.cancelAndJoin()
            }
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

            eventsLogger?.trace("mongodb.poll.polling", tags = context.toEventTags())
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
                            mongoDbPollMeterRegistry?.recordTimeToResponse(System.nanoTime() - requestStart)
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
                            "mongodb.poll.failure",
                            arrayOf(error, duration),
                            tags = context.toEventTags()
                        )
                        mongoDbPollMeterRegistry?.countFailure()

                        latch.cancel()
                    }

                    override fun onComplete() {
                        val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                        coroutineScope.launch {
                            eventsLogger?.info(
                                "mongodb.poll.successful-response",
                                arrayOf(duration, results.size),
                                tags = context.toEventTags()
                            )
                            mongoDbPollMeterRegistry?.countSuccess()
                            mongoDbPollMeterRegistry?.countRecords(results.size)

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
