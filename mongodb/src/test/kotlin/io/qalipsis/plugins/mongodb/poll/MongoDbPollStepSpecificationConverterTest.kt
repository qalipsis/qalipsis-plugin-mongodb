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

package io.qalipsis.plugins.mongodb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.invokeInvisible
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.plugins.mongodb.Sorting
import io.qalipsis.plugins.mongodb.converters.MongoDbDocumentPollBatchConverter
import io.qalipsis.plugins.mongodb.converters.MongoDbDocumentPollSingleConverter
import io.qalipsis.plugins.mongodb.poll.MongoDbIterativeReader
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration

/**
 *
 * @author Maxim Golokhov
 */
@WithMockk
internal class MongoDbPollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<MongoDbPollStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var mockedClientBuilder: () -> com.mongodb.reactivestreams.client.MongoClient

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<MongoDbPollStepSpecificationImpl>()))
            .isTrue()
    }

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(5)
    fun `should convert with name and metrics`() = testDispatcherProvider.runTest {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect(mockedClientBuilder)
            search {
                database = "db"
                collection = "col"
                query = Document()
                sort = linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                tieBreaker = "device"
            }
            monitoring {
                meters = true
                events = true
            }
            pollDelay(10_000L)
            broadcast(123, Duration.ofSeconds(20))
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        val recordsConverter: DatasourceObjectConverter<MongoDBQueryResult, out Any> = relaxedMockk()
        every { spiedConverter["buildConverter"](refEq(spec)) } returns recordsConverter

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbPollStepSpecificationImpl>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(recordsConverter)
                prop("reader").isNotNull().isInstanceOf(MongoDbIterativeReader::class).all {
                    prop("clientBuilder").isSameAs(mockedClientBuilder)
                    prop("meterRegistry").isNotNull().isEqualTo(meterRegistry)
                    prop("eventsLogger").isNotNull().isEqualTo(eventsLogger)
                }
            }
        }
        verifyOnce { spiedConverter["buildConverter"](refEq(spec)) }

        val channelFactory = creationContext.createdStep!!
            .getProperty<MongoDbIterativeReader>("reader")
            .getProperty<() -> Channel<List<Document>>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    internal fun `should build batch converter`() {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.search {
            database = "db"
            collection = "coll"
        }
        spec.flattenOutput = false

        // when
        val converter =
            converter.invokeInvisible<DatasourceObjectConverter<MongoDBQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(MongoDbDocumentPollBatchConverter::class).all {
            prop("databaseName").isEqualTo("db")
            prop("collectionName").isEqualTo("coll")
        }
    }

    @Test
    internal fun `should build single converter`() {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.search {
            database = "db"
            collection = "coll"
        }
        spec.flattenOutput = true

        // when
        val converter =
            converter.invokeInvisible<DatasourceObjectConverter<MongoDBQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(MongoDbDocumentPollSingleConverter::class).all {
            prop("databaseName").isEqualTo("db")
            prop("collectionName").isEqualTo("coll")
        }
    }

}
