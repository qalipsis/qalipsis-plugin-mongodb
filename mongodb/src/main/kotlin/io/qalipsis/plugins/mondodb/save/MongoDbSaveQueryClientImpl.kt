package io.qalipsis.plugins.mondodb.save

import com.mongodb.client.result.InsertManyResult
import com.mongodb.reactivestreams.client.MongoClient
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger


/**
 * Implementation of [MongoDbSaveQueryClient].
 * Client to query to MongoDB.
 *
 * @property clientBuilder supplier for the MongoDb client.
 * @property mongoDbSaveMeterRegistry the metrics for the query operation.
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSaveQueryClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientBuilder: () -> MongoClient,
    private val mongoDbSaveMeterRegistry: MongoDbSaveQueryMeterRegistry?,
    private var eventsLogger: EventsLogger?
) : MongoDbSaveQueryClient {

    private lateinit var client: MongoClient

    override suspend fun start() {
        client = clientBuilder()
    }

    override suspend fun execute(
        dbName: String,
        collName: String,
        records: List<Document>,
        contextEventTags: Map<String, String>
    ): MongoDbSaveQueryMeters {
        val result = Slot<Result<MongoDbSaveQueryMeters>>()
        val isFirst = AtomicBoolean(true)

        eventsLogger?.info("mongodb.save.saving-records", records.size, tags = contextEventTags)
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
                        mongoDbSaveMeterRegistry?.recordTimeToResponse(System.nanoTime() - requestStart)
                        isFirst.set(false)
                    }
                    saved.addAndGet(result.insertedIds.size)
                }

                override fun onError(error: Throwable) {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.warn("mongodb.save.failure", arrayOf(error, duration), tags = contextEventTags)
                    mongoDbSaveMeterRegistry?.countFailure(records.size)

                    ioCoroutineScope.launch {
                        result.set(Result.failure(error))
                    }
                }

                override fun onComplete() {
                    val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                    eventsLogger?.info(
                        "mongodb.save.successful-response",
                        arrayOf(duration, saved),
                        tags = contextEventTags
                    )
                    val failed = records.size - saved.get()
                    mongoDbSaveMeterRegistry?.countRecords(saved.get())
                    if (failed > 0) {
                        mongoDbSaveMeterRegistry?.countFailure(failed)
                        eventsLogger?.warn(
                            "mongodb.save.failed-response",
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

    override suspend fun stop() {
        tryAndLog(log) {
            client.close()
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
