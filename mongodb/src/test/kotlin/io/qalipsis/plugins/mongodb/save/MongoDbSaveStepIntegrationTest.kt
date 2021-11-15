package io.qalipsis.plugins.mongodb.save

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import io.mockk.confirmVerified
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.mondodb.save.MongoDbSaveQueryClientImpl
import io.qalipsis.plugins.mondodb.save.MongoDbSaveQueryMeterRegistry
import io.qalipsis.plugins.mongodb.Constants.DOCKER_IMAGE
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import kotlin.math.pow

/**
 *
 * @author Alexander Sosnovsky
 */
@Testcontainers
@WithMockk
internal class MongoDbSaveStepIntegrationTest {

    private lateinit var client: MongoClient

    val testDispatcherProvider = TestDispatcherProvider()

    @BeforeAll
    fun init() {
        client = MongoClients.create("mongodb://localhost:${mongodb.getMappedPort(27017)}/?streamType=netty")
    }

    @AfterAll
    fun shutDown() {
        client.close()
    }

    @RelaxedMockK
    private lateinit var queryMeterRegistry: MongoDbSaveQueryMeterRegistry
    private val eventsLogger = relaxedMockk<EventsLogger>()

    @Test
    @Timeout(10)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        val countLatch = SuspendedCountLatch(1)
        val results = ArrayList<Document>()
        val document = Document("key1", "val1")
        val saveClient = MongoDbSaveQueryClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { client },
            mongoDbSaveMeterRegistry = queryMeterRegistry,
            eventsLogger = eventsLogger
        )
        val tags: Map<String, String> = emptyMap()

        saveClient.start()

        saveClient.execute("db1", "col1", listOf(document), tags)

        fetchResult(client, "db1", "col1", results, countLatch)
        countLatch.await()
        assertThat(results).all {
            hasSize(1)
            index(0).all {
                key("key1").isEqualTo("val1")
            }
        }
        verify {
            queryMeterRegistry.recordTimeToResponse(more(0L))
            queryMeterRegistry.countRecords(eq(1))
        }
        confirmVerified(queryMeterRegistry)
    }

    @Test
    @Timeout(10)
    fun `should throw an exception when sending invalid documents`(): Unit = testDispatcherProvider.run {
        val saveClient = MongoDbSaveQueryClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { client },
            mongoDbSaveMeterRegistry = queryMeterRegistry,
            eventsLogger = eventsLogger
        )
        val tags: Map<String, String> = emptyMap()
        saveClient.start()

        assertThrows<Exception> {
            saveClient.execute(
                "db2",
                "col2",
                listOf(Document("key1", Duration.ZERO)), // Duration is not supported.
                tags
            )
        }
        verify {
            queryMeterRegistry.countFailure(eq(1))
        }
    }

    private fun fetchResult(
        client: MongoClient, database: String, collection: String, results: ArrayList<Document>,
        countLatch: SuspendedCountLatch
    ) {
        client.run {
            getDatabase(database)
                .getCollection(collection)
                .find(Document())
                .subscribe(
                    object : Subscriber<Document> {
                        override fun onSubscribe(s: Subscription) {
                            s.request(Long.MAX_VALUE)
                        }

                        override fun onNext(document: Document) {
                            results.add(document)
                            countLatch.blockingDecrement()
                        }

                        override fun onError(error: Throwable) {}

                        override fun onComplete() {}
                    }
                )
        }
    }

    companion object {

        @Container
        @JvmStatic
        val mongodb = MongoDBContainer(DockerImageName.parse(DOCKER_IMAGE))
            .apply {
                waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
            }
    }
}
