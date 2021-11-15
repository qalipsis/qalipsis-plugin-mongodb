package io.qalipsis.plugins.mongodb.search

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import com.mongodb.reactivestreams.client.MongoClient
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.mondodb.MongoDbRecord
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.search.MongoDbQueryClientImpl
import io.qalipsis.plugins.mondodb.search.MongoDbQueryMeterRegistry
import io.qalipsis.plugins.mongodb.configuration.AbstractMongoDbIntegrationTest
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

/**
 *
 * @author Alexander Sosnovsky
 */
@WithMockk
internal class MongoDbSearchStepIntegrationTest : AbstractMongoDbIntegrationTest() {

    @RelaxedMockK
    private lateinit var queryMeterRegistry: MongoDbQueryMeterRegistry
    private val eventsLogger = relaxedMockk<EventsLogger>()

    @RelaxedMockK
    private lateinit var context: StepContext<Any, Pair<Any, List<MongoDbRecord>>>

    @Test
    @Timeout(50)
    fun `should succeed when sending query with multiple results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")

        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client

        val searchClient = MongoDbQueryClientImpl(
            ioCoroutineScope = this,
            clientFactory = clientFactory,
            mongoDbMeterRegistry = queryMeterRegistry,
            eventsLogger = eventsLogger
        )

        searchClient.init()

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

        verify {
            queryMeterRegistry.recordTimeToResponse(more(0L))
            queryMeterRegistry.countRecords(eq(3))
            queryMeterRegistry.countSuccess()
        }

        confirmVerified(queryMeterRegistry)
    }

    @Test
    @Timeout(50)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")
        val clientFactory: () -> MongoClient = relaxedMockk()
        every { clientFactory.invoke() } returns client

        val searchClient = MongoDbQueryClientImpl(
            ioCoroutineScope = this,
            clientFactory = clientFactory,
            mongoDbMeterRegistry = queryMeterRegistry,
            eventsLogger = eventsLogger
        )

        searchClient.init()

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
            queryMeterRegistry.recordTimeToResponse(more(0L))
            queryMeterRegistry.countRecords(eq(1))
            queryMeterRegistry.countSuccess()

        }

        confirmVerified(queryMeterRegistry)
    }
}
