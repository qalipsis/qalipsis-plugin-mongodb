package io.qalipsis.plugins.mongodb.poll


import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.poll.MongoDbIterativeReader
import io.qalipsis.plugins.mondodb.poll.MongoDbPollStatement
import io.qalipsis.plugins.mongodb.configuration.AbstractMongoDbIntegrationTest
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

@WithMockk
internal class MongoDbIterativeReaderIntegrationTest : AbstractMongoDbIntegrationTest() {

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private lateinit var reader: MongoDbIterativeReader

    @AfterEach
    @Timeout(5)
    fun afterEach() {
        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(20)
    fun `should sort correctly by device and event`() = testDispatcherProvider.run {
        populateMongoFromCsv("input/all_documents.csv")

        val client: (() -> com.mongodb.reactivestreams.client.MongoClient) =
            { MongoClients.create("mongodb://localhost:${mongodb.getMappedPort(27017)}/?streamType=netty") }
        val pollStatement = MongoDbPollStatement(
            databaseName = "db",
            collectionName = "col",
            findClause = Document(),
            sortClauseValues = linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC),
            tieBreakerName = "device"
        )
        reader = MongoDbIterativeReader(
            clientBuilder = client,
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null
        )

        reader.start(relaxedMockk())

        Assertions.assertTrue(reader.hasNext())

        val received = mutableListOf<Document>()
        reader.next().documents.forEach(received::add)

        assertThat(received).all {
            hasSize(39)
            index(0).all {
                key("device").isEqualTo("Car #1")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
            index(12).all {
                key("device").isEqualTo("Car #1")
                key("event").isEqualTo("Stop")
                key("time").isEqualTo(BsonTimestamp(1603198728000))
            }
            index(26).all {
                key("device").isEqualTo("Truck #1")
                key("event").isEqualTo("Driving")
                key("time").isEqualTo(BsonTimestamp(1603197368000))
            }
            index(38).all {
                key("device").isEqualTo("Truck #1")
                key("event").isEqualTo("Stop")
                key("time").isEqualTo(BsonTimestamp(1603198728000))
            }
        }

        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(10)
    fun `should search by findClause`() = testDispatcherProvider.run {

        populateMongoFromCsv("input/all_documents.csv")

        val client: (() -> com.mongodb.reactivestreams.client.MongoClient) =
            { MongoClients.create("mongodb://localhost:${mongodb.getMappedPort(27017)}/?streamType=netty") }

        val pollStatement = MongoDbPollStatement(
            databaseName = "db",
            collectionName = "col",
            findClause = Document("device", "Truck #1"),
            sortClauseValues = linkedMapOf("time" to Sorting.ASC),
            tieBreakerName = "time"
        )
        reader = MongoDbIterativeReader(
            clientBuilder = client,
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null
        )

        reader.start(relaxedMockk())

        Assertions.assertTrue(reader.hasNext())

        val received = mutableListOf<Document>()
        reader.next().documents.forEach(received::add)

        assertThat(received.count { "Truck #1" == it["device"] }).isEqualTo(13)

        reader.stop(relaxedMockk())
    }

}
