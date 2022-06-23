package io.qalipsis.plugins.mongodb.search

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.mongodb.reactivestreams.client.MongoClients
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.mondodb.*
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension


/**
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSearchStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    fun `should add minimal configuration for the step`() = testDispatcherProvider.run {
        val previousStep = DummyStepSpecification()
        previousStep.mongodb().search {
            name = "my-search-step"
            connect {
                MongoClients.create()
            }
            search {
                database = { _, _ -> "db" }
                collection = { _, _ -> "col" }
                query = { _, _ -> Document() }
                sort = { _, _ -> linkedMapOf("asc" to Sorting.ASC, "desc" to Sorting.DESC) }
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(MongoDbSearchStepSpecificationImpl::class).all {
            prop("name") { MongoDbSearchStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-search-step")
            prop(MongoDbSearchStepSpecificationImpl<*>::clientFactory).isNotNull()
            prop(MongoDbSearchStepSpecificationImpl<*>::searchConfig).isNotNull().all {
                prop(MongoDbQueryConfiguration<*>::database).isNotNull()
                prop(MongoDbQueryConfiguration<*>::collection).isNotNull()
                prop(MongoDbQueryConfiguration<*>::query).isNotNull()
                prop(MongoDbQueryConfiguration<*>::sort).isNotNull()
            }
            prop(MongoDbSearchStepSpecificationImpl<*>::monitoringConfig).isNotNull().all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(MongoDbSearchStepSpecificationImpl<*>::flattenOutput).isFalse()
        }

        val step: MongoDbSearchStepSpecificationImpl<*> =
            previousStep.nextSteps[0] as MongoDbSearchStepSpecificationImpl<*>

        val database = step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("database")
        assertThat(database(relaxedMockk(), relaxedMockk())).isEqualTo("db")

        val collection =
            step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("collection")
        assertThat(collection(relaxedMockk(), relaxedMockk())).isEqualTo("col")

        val query = step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> Document>("query")
        assertThat(query(relaxedMockk(), relaxedMockk())).isEqualTo(Document())

        val sort =
            step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> LinkedHashMap<String, Sorting>>(
                "sort"
            )
        assertThat(sort(relaxedMockk(), relaxedMockk())).isEqualTo(
            linkedMapOf("asc" to Sorting.ASC, "desc" to Sorting.DESC)
        )
    }


    @Test
    fun `should add a complete configuration for the step`() = testDispatcherProvider.run {
        val previousStep = DummyStepSpecification()
        previousStep.mongodb().search {
            name = "my-search-step"
            connect {
                MongoClients.create()
            }
            search {
                database = { _, _ -> "db" }
                collection = { _, _ -> "col" }
                query = { _, _ -> Document("device", "Truck #1") }
                sort = { _, _ -> linkedMapOf("desc" to Sorting.DESC) }
            }

            monitoring {
                events = true
                meters = true
            }

        }.flatten()

        assertThat(previousStep.nextSteps[0]).isInstanceOf(MongoDbSearchStepSpecificationImpl::class).all {
            prop("name") { MongoDbSearchStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-search-step")
            prop(MongoDbSearchStepSpecificationImpl<*>::clientFactory).isNotNull()
            prop(MongoDbSearchStepSpecificationImpl<*>::searchConfig).isNotNull().all {
                prop(MongoDbQueryConfiguration<*>::database).isNotNull()
                prop(MongoDbQueryConfiguration<*>::collection).isNotNull()
                prop(MongoDbQueryConfiguration<*>::query).isNotNull()
                prop(MongoDbQueryConfiguration<*>::sort).isNotNull()
            }
            prop(MongoDbSearchStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            prop(MongoDbSearchStepSpecificationImpl<*>::flattenOutput).isTrue()
        }

        val step: MongoDbSearchStepSpecificationImpl<*> =
            previousStep.nextSteps[0] as MongoDbSearchStepSpecificationImpl<*>

        val database = step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("database")
        assertThat(database(relaxedMockk(), relaxedMockk())).isEqualTo("db")

        val collection =
            step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("collection")
        assertThat(collection(relaxedMockk(), relaxedMockk())).isEqualTo("col")

        val query = step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> Document>("query")
        assertThat(query(relaxedMockk(), relaxedMockk())).isEqualTo(Document("device", "Truck #1"))

        val sort =
            step.searchConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> LinkedHashMap<String, Sorting>>(
                "sort"
            )
        assertThat(sort(relaxedMockk(), relaxedMockk())).isEqualTo(linkedMapOf("desc" to Sorting.DESC))
    }
}
