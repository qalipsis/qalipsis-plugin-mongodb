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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.key
import com.mongodb.reactivestreams.client.MongoClient
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.Timer
import io.qalipsis.plugins.mongodb.MongoDbQueryMeters
import io.qalipsis.plugins.mongodb.MongoDbRecord
import io.qalipsis.plugins.mongodb.Sorting
import io.qalipsis.plugins.mongodb.configuration.AbstractMongoDbIntegrationTest
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit

/**
 *
 * @author Alexander Sosnovsky
 */
@WithMockk
internal class MongoDbSearchStepIntegrationTest : AbstractMongoDbIntegrationTest() {

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private val timeToResponse = relaxedMockk<Timer>()

    private val recordsCount = relaxedMockk<Counter>()

    private val successCounter = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()

    @RelaxedMockK
    private lateinit var context: StepContext<Any, Pair<Any, List<MongoDbRecord>>>

    @Test
    @Timeout(50)
    fun `should succeed when sending query with multiple results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")
        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client
        val metersTags: Map<String, String> = emptyMap()

        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-received-records",
                    refEq(metersTags)
                )
            } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-success",
                    refEq(metersTags)
                )
            } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-failure",
                    refEq(metersTags)
                )
            } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
            every {
                timer(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-time-to-response",
                    refEq(metersTags)
                )
            } returns timeToResponse
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }

        val searchClient = MongoDbQueryClientImpl(
            clientFactory = clientFactory,
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )

        searchClient.start(startStopContext)

        val results = searchClient.execute(
            "db", "col", Document("time", BsonTimestamp(1603197368000)),
            linkedMapOf("device" to Sorting.DESC),
            context.toEventTags()
        )

        assertThat(results.documents).all {
            hasSize(3)
            index(0).all {
                key("device").isEqualTo("Truck #1")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
            index(1).all {
                key("device").isEqualTo("Car #2")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
            index(2).all {
                key("device").isEqualTo("Car #1")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
        }
        assertThat(results.meters).isInstanceOf(MongoDbQueryMeters::class.java).all {
            prop("fetchedRecords").isEqualTo(3)
            prop("timeToResult").isNotNull()
        }

        verify {
            timeToResponse.record(more(0L), TimeUnit.NANOSECONDS)
            recordsCount.increment(3.0)
            successCounter.increment()
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            successCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            eventsLogger.debug("mongodb.search.searching", any(), any(), tags = metersTags)
            eventsLogger.info("mongodb.search.time-to-response", any(), any(), tags = metersTags)
            eventsLogger.info("mongodb.search.success", any(), any(), tags = metersTags)
        }

        confirmVerified(timeToResponse, recordsCount, successCounter, eventsLogger)
    }

    @Test
    @Timeout(50)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")
        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client

        val metersTags: Map<String, String> = emptyMap()

        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-received-records",
                    refEq(metersTags)
                )
            } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-success",
                    refEq(metersTags)
                )
            } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every {
                counter(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-failure",
                    refEq(metersTags)
                )
            } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
            every {
                timer(
                    "test-scenario",
                    "test-step",
                    "mongodb-search-time-to-response",
                    refEq(metersTags)
                )
            } returns timeToResponse
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }

        val searchClient = MongoDbQueryClientImpl(
            clientFactory = clientFactory,
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )

        searchClient.start(startStopContext)

        val results = searchClient.execute(
            "db", "col",
            Document("time", BsonTimestamp(1603197368000)).append("device", "Truck #1"),
            linkedMapOf("device" to Sorting.DESC),
            context.toEventTags()
        )
        assertThat(results.documents).all {
            hasSize(1)
            index(0).all {
                key("device").isEqualTo("Truck #1")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
        }

        verify {
            timeToResponse.record(more(0L), TimeUnit.NANOSECONDS)
            recordsCount.increment(1.0)
            successCounter.increment()
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            successCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            eventsLogger.debug("mongodb.search.searching", any(), any(), tags = metersTags)
            eventsLogger.info("mongodb.search.time-to-response", any(), any(), tags = metersTags)
            eventsLogger.info("mongodb.search.success", any(), any(), tags = metersTags)
        }

        confirmVerified(timeToResponse, recordsCount, successCounter, eventsLogger)
    }
}
