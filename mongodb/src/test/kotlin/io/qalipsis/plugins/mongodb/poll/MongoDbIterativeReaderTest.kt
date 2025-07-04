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
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.impl.annotations.SpyK
import io.mockk.spyk
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration

@WithMockk
internal class MongoDbIterativeReaderTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

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
    private lateinit var eventsLogger: EventsLogger

    @RelaxedMockK
    private lateinit var meterRegistry: CampaignMeterRegistry

    private val recordsCount = relaxedMockk<Counter>()

    private val successCounter = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()


    @Test
    @Timeout(25)
    internal fun `should be restartable`() = testDispatcherProvider.run {
        // given
        val latch = SuspendedCountLatch(1, true)
        val tags: Map<String, String> = emptyMap()
        val mockMeterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("test-scenario", "test-step", "mongodb-poll-received-records", refEq(tags)) } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every { counter("test-scenario", "test-step","mongodb-poll-successes", refEq(tags)) } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { counter("test-scenario", "test-step","mongodb-poll-failures", refEq(tags)) } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns tags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }
        val reader = spyk(
            MongoDbIterativeReader(
                clientBuilder = clientBuilder,
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(300),
                resultsChannelFactory = resultsChannelFactory,
                coroutineScope = this,
                eventsLogger = eventsLogger,
                meterRegistry = mockMeterRegistry,
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](refEq(client)) } coAnswers { latch.decrement() }

        // when
        reader.start(startStopContext)

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
        reader.stop(startStopContext)
        verify { client.close() }
        verify { resultsChannel.cancel() }
        verify { pollStatement.reset() }
        // then
        assertThat(reader.hasNext()).isFalse()

        // when
        latch.reset()
        reader.start(startStopContext)
        // then
        verify { reader["init"]() }
        verify { resultsChannelFactory() }
        assertThat(reader.hasNext()).isTrue()
        assertThat(reader.getProperty<Job>("pollingJob")).isNotSameAs(pollingJob)
        assertThat(reader.getProperty<MongoClient>("client")).isSameAs(client)
        assertThat(reader.getProperty<Channel<List<Document>>>("resultsChannel")).isSameAs(resultsChannel)

        reader.stop(startStopContext)
    }

    @Test
    @Timeout(10)
    fun `should be empty before start`() = testDispatcherProvider.runTest {
        // given
        val reader = MongoDbIterativeReader(
            clientBuilder = clientBuilder,
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = resultsChannelFactory,
            coroutineScope = this,
            eventsLogger = eventsLogger,
            meterRegistry = meterRegistry
        )

        // then
        assertThat(reader.hasNext()).isFalse()
        assertThat(reader.getProperty<Channel<List<Document>>>("resultsChannel")).isNull()
    }

    @Test
    @Timeout(20)
    fun `should poll at least twice after start`() = testDispatcherProvider.run {
        // given
        val tags: Map<String, String> = emptyMap()
        val mockMeterRegistry = relaxedMockk<CampaignMeterRegistry> {
            every { counter("test-scenario", "test-step","mongodb-poll-received-records", refEq(tags)) } returns recordsCount
            every { recordsCount.report(any()) } returns recordsCount
            every { counter("test-scenario", "test-step","mongodb-poll-successes", refEq(tags)) } returns successCounter
            every { successCounter.report(any()) } returns successCounter
            every { counter("test-scenario", "test-step","mongodb-poll-failures", refEq(tags)) } returns failureCounter
            every { failureCounter.report(any()) } returns failureCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns tags
            every { scenarioName } returns "test-scenario"
            every { stepName } returns "test-step"
        }
        val reader = spyk(
            MongoDbIterativeReader(
                clientBuilder = clientBuilder,
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(100),
                resultsChannelFactory = resultsChannelFactory,
                coroutineScope = this,
                eventsLogger = eventsLogger,
                meterRegistry = mockMeterRegistry
            ), recordPrivateCalls = true
        )
        val countDownLatch = SuspendedCountLatch(2, true)
        coEvery { reader["poll"](any<MongoClient>()) } coAnswers { countDownLatch.decrement() }

        // when
        reader.start(startStopContext)
        countDownLatch.await()

        // then
        coVerify(atLeast = 2) { reader["poll"](any<MongoClient>()) }
        assertThat(reader.hasNext()).isTrue()

        reader.stop(startStopContext)
    }
}
