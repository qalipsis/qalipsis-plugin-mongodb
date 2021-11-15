package io.qalipsis.plugins.mongodb.poll

import assertk.assertThat
import assertk.assertions.isFalse
import assertk.assertions.isNotNull
import assertk.assertions.isNotSameAs
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import com.mongodb.reactivestreams.client.MongoClient
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.impl.annotations.SpyK
import io.mockk.spyk
import io.mockk.verify
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.poll.MongoDbIterativeReader
import io.qalipsis.plugins.mondodb.poll.MongoDbPollStatement
import io.qalipsis.plugins.mondodb.search.MongoDbQueryMeterRegistry
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runBlockingTest
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

@WithMockk
internal class MongoDbIterativeReaderTest {

    @RelaxedMockK
    private lateinit var client: MongoClient

    private val clientBuilder: () -> MongoClient = { client }

    @RelaxedMockK
    private lateinit var resultsChannel: Channel<MongoDBQueryResult>

    @SpyK
    private var resultsChannelFactory: () -> Channel<MongoDBQueryResult> = { resultsChannel }

    @RelaxedMockK
    private lateinit var pollStatement: MongoDbPollStatement

    @RelaxedMockK
    private lateinit var queryMeterRegistry: MongoDbQueryMeterRegistry

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    @Test
    @Timeout(25)
    internal fun `should be restartable`() = runBlocking {
        // given
        val latch = SuspendedCountLatch(1, true)
        val reader = spyk(
            MongoDbIterativeReader(
                clientBuilder = clientBuilder,
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(300),
                resultsChannelFactory = resultsChannelFactory,
                eventsLogger = eventsLogger,
                mongoDbPollMeterRegistry = queryMeterRegistry,
                coroutineScope = this
            ), recordPrivateCalls = true
        )

        coEvery { reader["poll"](refEq(client)) } coAnswers { latch.decrement() }


        // when
        reader.start(relaxedMockk { })

        // then
        latch.await()
        verify { reader["init"]() }
        verify { resultsChannelFactory() }
        assertThat(reader.hasNext()).isTrue()
        val pollingJob = reader.getProperty<Job>("pollingJob")
        assertThat(pollingJob).isNotNull()
        val client = reader.getProperty<MongoClient>("client")
        assertThat(client).isNotNull()
        val resultsChannel = reader.getProperty<Channel<List<Document>>>("resultsChannel")
        assertThat(resultsChannel).isSameAs(resultsChannel)

        // when
        reader.stop(relaxedMockk())
        verify { client.close() }
        verify { resultsChannel.cancel() }
        verify { pollStatement.reset() }
        // then
        assertThat(reader.hasNext()).isFalse()

        // when
        latch.reset()
        reader.start(relaxedMockk { })
        // then
        verify { reader["init"]() }
        verify { resultsChannelFactory() }
        assertThat(reader.hasNext()).isTrue()
        assertThat(reader.getProperty<Job>("pollingJob")).isNotSameAs(pollingJob)
        assertThat(reader.getProperty<MongoClient>("client")).isSameAs(client)
        assertThat(reader.getProperty<Channel<List<Document>>>("resultsChannel")).isSameAs(resultsChannel)

        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(10)
    fun `should be empty before start`() = runBlockingTest {
        // given
        val reader = MongoDbIterativeReader(
            clientBuilder = clientBuilder,
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = resultsChannelFactory,
            eventsLogger = eventsLogger,
            mongoDbPollMeterRegistry = queryMeterRegistry,
            coroutineScope = this
        )

        // then
        assertThat(reader.hasNext()).isFalse()
        assertThat(reader.getProperty<Channel<List<Document>>>("resultsChannel")).isNull()
    }

    @Test
    @Timeout(20)
    fun `should poll at least twice after start`() = runBlocking {
        // given
        val reader = spyk(
            MongoDbIterativeReader(
                clientBuilder = clientBuilder,
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(100),
                resultsChannelFactory = resultsChannelFactory,
                eventsLogger = eventsLogger,
                mongoDbPollMeterRegistry = queryMeterRegistry,
                coroutineScope = this
            ), recordPrivateCalls = true
        )
        val countDownLatch = SuspendedCountLatch(2, true)
        coEvery { reader["poll"](any<MongoClient>()) } coAnswers { countDownLatch.decrement() }

        // when
        reader.start(relaxedMockk())
        countDownLatch.await()

        // then
        coVerify(atLeast = 2) { reader["poll"](any<MongoClient>()) }
        assertThat(reader.hasNext()).isTrue()

        reader.stop(relaxedMockk())
    }
}
