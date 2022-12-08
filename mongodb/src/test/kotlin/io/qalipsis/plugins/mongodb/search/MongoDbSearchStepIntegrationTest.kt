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
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
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

    @RelaxedMockK
    private lateinit var context: StepContext<Any, Pair<Any, List<MongoDbRecord>>>

    @Test
    @Timeout(50)
    fun `should succeed when sending query with multiple results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")
        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client

        val metersTags = relaxedMockk<Tags>()
        val tags: Map<String, String> = emptyMap()

        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("mongodb-search-received-records", refEq(metersTags)) } returns recordsCount
            every { counter("mongodb-search-success", refEq(metersTags)) } returns successCounter
            every { timer("mongodb-search-time-to-response", refEq(metersTags)) } returns timeToResponse
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        val searchClient = MongoDbQueryClientImpl(
            ioCoroutineScope = this,
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

            eventsLogger.debug("mongodb.search.searching", any(), any(), tags = tags)
            eventsLogger.info("mongodb.search.time-to-response", any(), any(), tags = tags)
            eventsLogger.info("mongodb.search.success", any(), any(), tags = tags)
        }

        confirmVerified(timeToResponse, recordsCount, successCounter, eventsLogger)
    }

    @Test
    @Timeout(50)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")
        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client

        val metersTags = relaxedMockk<Tags>()
        val tags: Map<String, String> = emptyMap()

        val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("mongodb-search-received-records", refEq(metersTags)) } returns recordsCount
            every { counter("mongodb-search-success", refEq(metersTags)) } returns successCounter
            every { timer("mongodb-search-time-to-response", refEq(metersTags)) } returns timeToResponse
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        val searchClient = MongoDbQueryClientImpl(
            ioCoroutineScope = this,
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

            eventsLogger.debug("mongodb.search.searching", any(), any(), tags = tags)
            eventsLogger.info("mongodb.search.time-to-response", any(), any(), tags = tags)
            eventsLogger.info("mongodb.search.success", any(), any(), tags = tags)
        }

        confirmVerified(timeToResponse, recordsCount, successCounter, eventsLogger)
    }
}
