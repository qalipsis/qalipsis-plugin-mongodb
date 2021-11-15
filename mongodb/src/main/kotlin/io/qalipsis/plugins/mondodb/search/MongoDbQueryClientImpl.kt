package io.qalipsis.plugins.mondodb.search

import com.mongodb.reactivestreams.client.MongoClient
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
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Implementation of [MongoDbQueryClient].
 * Client to query from MongoDB.
 *
 * @property clientFactory supplier for the MongoDb client
 * @property mongoDbMeterRegistry the metrics for the query operation
 * @property eventsLogger the logger for events to track what happens during save query execution.
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbQueryClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientFactory: () -> MongoClient,
    private val mongoDbMeterRegistry: MongoDbQueryMeterRegistry?,
    private var eventsLogger: EventsLogger?
) : MongoDbQueryClient {

    private lateinit var client: MongoClient

    /**
     * Prepare client inside before execute
     */
    override suspend fun init() {
        client = clientFactory()
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

        eventsLogger?.debug("mongodb.search.searching", tags = contextEventTags)
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
                        mongoDbMeterRegistry?.recordTimeToResponse(System.nanoTime() - requestStart)
                        isFirst.set(false)
                    }
                    results.add(document)
                }

                override fun onError(error: Throwable) {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.warn("mongodb.search.failure", arrayOf(error, duration), tags = contextEventTags)
                    mongoDbMeterRegistry?.countFailure()

                    ioCoroutineScope.launch {
                        latch.set(Result.failure(error))
                    }
                }

                override fun onComplete() {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.info(
                        "mongodb.search.successful-response",
                        arrayOf(duration, results.size),
                        tags = contextEventTags
                    )
                    mongoDbMeterRegistry?.countSuccess()
                    mongoDbMeterRegistry?.countRecords(results.size)

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
    override suspend fun stop() {
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


