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
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.search.MongoDbQueryConfiguration
import io.qalipsis.plugins.mondodb.search.MongoDbSearchStep
import io.qalipsis.plugins.mondodb.search.MongoDbSearchStepSpecificationConverter
import io.qalipsis.plugins.mondodb.search.MongoDbSearchStepSpecificationImpl
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.CoroutineScope
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

    @RelaxedMockK
    private lateinit var ioCoroutineScope: CoroutineScope

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
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
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
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("clientFactory").isNotNull().isSameAs(clientFactory)
                    prop("meterRegistry").isNull()
                    prop("eventsLogger").isNotNull().isSameAs(eventsLogger)
                }
            }
        }
    }

}
