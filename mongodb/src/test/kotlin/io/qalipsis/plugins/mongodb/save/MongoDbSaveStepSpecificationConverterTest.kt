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

@file:Suppress("UNCHECKED_CAST")

package io.qalipsis.plugins.mongodb.save

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import com.mongodb.reactivestreams.client.MongoClient
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 *
 * @author Alexander Sosnovsky
 */
@WithMockk
internal class MongoDbSaveStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<MongoDbSaveStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val databaseName: (suspend (ctx: StepContext<*, *>, input: Any?) -> String) = { _, _ -> "db" }

    private val collectionName: (suspend (ctx: StepContext<*, *>, input: Any) -> String) = { _, _ -> "col" }

    private val recordSupplier: (suspend (ctx: StepContext<*, *>, input: Any?) -> List<Document>) = { _, _ ->
        listOf(
            Document("key1", "val1"),
            Document("key3", "val3"),
            Document("key3-1", "val3-1")
        )
    }

    @RelaxedMockK
    private lateinit var clientBuilder: () -> MongoClient


    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<MongoDbSaveStepSpecificationImpl<*>>()))
            .isTrue()

    }

    @Test
    fun `should convert with name, retry policy and meters`() = testDispatcherProvider.runTest {
        // given
        val spec = MongoDbSaveStepSpecificationImpl<Any>()
        spec.also {
            it.name = "mongodb-save-step"
            it.clientBuilder = clientBuilder
            it.query {
                database = databaseName
                collection = collectionName
                documents = recordSupplier
            }
            it.retryPolicy = mockedRetryPolicy
            it.monitoring {
                meters = true
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)
        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).all {
            prop("name").isNotNull().isEqualTo("mongodb-save-step")
            prop("mongoDbSaveQueryClient").all {
                prop("clientBuilder").isNotNull().isSameAs(clientBuilder)
                prop("meterRegistry").isNotNull().isSameAs(meterRegistry)
                prop("eventsLogger").isNotNull().isSameAs(eventsLogger)
            }
            prop("retryPolicy").isNotNull()
            prop("databaseName").isEqualTo(databaseName)
            prop("collectionName").isEqualTo(collectionName)
            prop("recordsFactory").isSameAs(recordSupplier)
        }
    }

    @Test
    fun `should convert without name and retry policy but with events`() = testDispatcherProvider.runTest {
        // given
        val spec = MongoDbSaveStepSpecificationImpl<Any>()
        spec.also {
            it.query {
                database = databaseName
                collection = collectionName
                documents = recordSupplier
            }
            it.clientBuilder = clientBuilder
            it.monitoring {
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)


        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbSaveStepSpecificationImpl<*>>
        )

        // then
        assertThat(creationContext.createdStep!!).all {
            prop("retryPolicy").isNull()
            prop("databaseName").isEqualTo(databaseName)
            prop("collectionName").isEqualTo(collectionName)
            prop("recordsFactory").isSameAs(recordSupplier)
            prop("mongoDbSaveQueryClient").all {
                prop("clientBuilder").isNotNull().isSameAs(clientBuilder)
                prop("meterRegistry").isNull()
                prop("eventsLogger").isNotNull().isSameAs(eventsLogger)
            }
        }
    }

    // TODO Test the builder method for the meter registry.
}
