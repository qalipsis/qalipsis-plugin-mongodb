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

package io.qalipsis.plugins.mongodb.search

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import com.mongodb.reactivestreams.client.MongoClient
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.plugins.mongodb.Sorting
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
internal class MongoDbSearchStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<MongoDbSearchStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val databaseName: (suspend (ctx: StepContext<*, *>, input: Any?) -> String) = { _, _ -> "db" }

    private val collectionName: (suspend (ctx: StepContext<*, *>, input: Any) -> String) = { _, _ -> "col" }

    private val filter: (suspend (ctx: StepContext<*, *>, input: Any) -> Document) = { _, _ -> Document() }

    private val sorting: (suspend (ctx: StepContext<*, *>, input: Any) -> LinkedHashMap<String, Sorting>) =
        { _, _ -> linkedMapOf("asc" to Sorting.ASC, "desc" to Sorting.DESC) }

    @RelaxedMockK
    private lateinit var clientFactory: () -> MongoClient

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<MongoDbSearchStepSpecificationImpl<*>>()))
            .isTrue()

    }

    @Test
    fun `should convert with name and retry policy`() = testDispatcherProvider.runTest {
        // given
        val spec = MongoDbSearchStepSpecificationImpl<Any>()
        spec.also {
            it.name = "mongodb-search-step"
            it.searchConfig = MongoDbQueryConfiguration(
                database = databaseName,
                collection = collectionName,
                query = filter,
                sort = sorting
            )
            it.clientFactory = clientFactory
            it.retryPolicy = mockedRetryPolicy
            it.monitoring {
                meters = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        converter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbSearchStepSpecificationImpl<*>>
        )

        // then
        creationContext.createdStep!!.let { it ->
            assertThat(it).isInstanceOf(MongoDbSearchStep::class).all {
                prop("name").isNotNull().isEqualTo("mongodb-search-step")
                prop("mongoDbQueryClient").all {
                    prop("clientFactory").isNotNull().isSameAs(clientFactory)
                    prop("eventsLogger").isNull()
                    prop("meterRegistry").isNotNull().isSameAs(meterRegistry)
                }
                prop("retryPolicy").isNotNull()
                prop("databaseName").isEqualTo(databaseName)
                prop("collectionName").isEqualTo(collectionName)
                prop("filter").isEqualTo(filter)
                prop("sorting").isEqualTo(sorting)
            }
        }
    }

    @Test
    fun `should convert without name and retry policy`() = testDispatcherProvider.runTest {
        // given
        val spec = MongoDbSearchStepSpecificationImpl<Any>()
        spec.also {
            it.searchConfig = MongoDbQueryConfiguration(
                database = databaseName,
                collection = collectionName,
                query = filter,
                sort = sorting
            )
            it.clientFactory = clientFactory
            it.monitoring {
                events = true
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        converter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbSearchStepSpecificationImpl<*>>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(MongoDbSearchStep::class).all {
                prop("name").isNotNull()
                prop("retryPolicy").isNull()
                prop("databaseName").isEqualTo(databaseName)
                prop("collectionName").isEqualTo(collectionName)
                prop("filter").isEqualTo(filter)
                prop("sorting").isEqualTo(sorting)
                prop("mongoDbQueryClient").all {
                    prop("clientFactory").isNotNull().isSameAs(clientFactory)
                    prop("meterRegistry").isNull()
                    prop("eventsLogger").isNotNull().isSameAs(eventsLogger)
                }
            }
        }
    }

}
