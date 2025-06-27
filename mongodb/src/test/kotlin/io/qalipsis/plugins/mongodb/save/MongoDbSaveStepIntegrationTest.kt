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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.key
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.mongodb.Constants.DOCKER_IMAGE
import io.qalipsis.test.assertk.prop
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
import java.util.concurrent.TimeUnit
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

    private val timeToResponse = relaxedMockk<Timer>()

    private val recordsCount = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()

    private val successCounter = relaxedMockk<Counter>()

    private val eventsLogger = relaxedMockk<EventsLogger>()

    @Test
    @Timeout(10)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        val metersTags = emptyMap<String, String>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-saving-records",
                    refEq(metersTags)
                )
            } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every {
                timer(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-time-to-response",
                    refEq(metersTags)
                )
            } returns timeToResponse
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-failures",
                    refEq(metersTags)
                )
            } returns failureCounter
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-successes",
                    refEq(metersTags)
                )
            } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { failureCounter.report(any()) } returns failureCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }
        val countLatch = SuspendedCountLatch(1)
        val results = ArrayList<Document>()
        val document = Document("key1", "val1")
        val saveClient = MongoDbSaveQueryClientImpl(
            clientBuilder = { client },
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )

        saveClient.start(startStopContext)

        val resultOfExecute = saveClient.execute("db1", "col1", listOf(document), metersTags)

        assertThat(resultOfExecute).isInstanceOf(MongoDbSaveQueryMeters::class.java).all {
            prop("savedRecords").isEqualTo(1)
            prop("failedRecords").isEqualTo(0)
            prop("failedRecords").isNotNull()
        }

        fetchResult(client, "db1", "col1", results, countLatch)
        countLatch.await()
        assertThat(results).all {
            hasSize(1)
            index(0).all {
                key("key1").isEqualTo("val1")
            }
        }

        verify {
            eventsLogger.debug("mongodb.save.saving-records", 1, any(), tags = metersTags)
            timeToResponse.record(more(0L), TimeUnit.NANOSECONDS)
            recordsCount.increment(1.0)
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            eventsLogger.info("mongodb.save.time-to-response", any<Duration>(), any(), tags = metersTags)
            eventsLogger.info("mongodb.save.saved-records", any<Array<*>>(), any(), tags = metersTags)
        }
        confirmVerified(timeToResponse, recordsCount, eventsLogger)
    }

    @Test
    @Timeout(10)
    fun `should throw an exception when sending invalid documents`(): Unit = testDispatcherProvider.run {
        val metersTags = emptyMap<String, String>()
        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-saving-records",
                    refEq(metersTags)
                )
            } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-failures",
                    refEq(metersTags)
                )
            } returns failureCounter
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-save-successes",
                    refEq(metersTags)
                )
            } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { failureCounter.report(any()) } returns failureCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }

        val saveClient = MongoDbSaveQueryClientImpl(
            clientBuilder = { client },
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        val tags: Map<String, String> = emptyMap()
        saveClient.start(startStopContext)

        assertThrows<Exception> {
            saveClient.execute(
                "db2",
                "col2",
                listOf(Document("key1", Duration.ZERO)), // Duration is not supported.
                tags
            )
        }
        verify {
            failureCounter.increment(1.0)
            failureCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
        }
        confirmVerified(failureCounter)
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
