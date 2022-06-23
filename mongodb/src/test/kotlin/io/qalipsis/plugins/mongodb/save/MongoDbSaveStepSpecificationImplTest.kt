package io.qalipsis.plugins.mongodb.save

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.mongodb.reactivestreams.client.MongoClients
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.mondodb.mongodb
import io.qalipsis.plugins.mondodb.save.MongoDbSaveQueryConfiguration
import io.qalipsis.plugins.mondodb.save.MongoDbSaveStepSpecificationImpl
import io.qalipsis.plugins.mondodb.save.save
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension


/**
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSaveStepSpecificationImplTest {

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

    @Test
    fun `should add minimal configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.mongodb().save {
            name = "my-save-step"
            connect {
                MongoClients.create()
            }
            query {
                database = databaseName
                collection = collectionName
                records = recordSupplier
            }

        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(MongoDbSaveStepSpecificationImpl::class).all {
            prop("name") { MongoDbSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(MongoDbSaveStepSpecificationImpl<*>::clientBuilder).isNotNull()
            prop(MongoDbSaveStepSpecificationImpl<*>::queryConfig).all {
                prop(MongoDbSaveQueryConfiguration<*>::records).isEqualTo(recordSupplier)
            }
            prop(MongoDbSaveStepSpecificationImpl<*>::monitoringConfig).isNotNull().all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
        }

        val step: MongoDbSaveStepSpecificationImpl<*> =
            previousStep.nextSteps[0] as MongoDbSaveStepSpecificationImpl<*>

        val database = step.queryConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("database")
        assertThat(database(relaxedMockk(), relaxedMockk())).isEqualTo("db")

        val collection =
            step.queryConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("collection")
        assertThat(collection(relaxedMockk(), relaxedMockk())).isEqualTo("col")
    }


    @Test
    fun `should add a complete configuration for the step`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.mongodb().save {
            name = "my-save-step"
            connect {
                MongoClients.create()
            }
            query {
                database = databaseName
                collection = collectionName
                records = recordSupplier
            }
            monitoring {
                events = true
                meters = true
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(MongoDbSaveStepSpecificationImpl::class).all {
            prop("name") { MongoDbSaveStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-save-step")
            prop(MongoDbSaveStepSpecificationImpl<*>::clientBuilder).isNotNull()
            prop(MongoDbSaveStepSpecificationImpl<*>::queryConfig).all {
                prop(MongoDbSaveQueryConfiguration<*>::records).isEqualTo(recordSupplier)
            }
            prop(MongoDbSaveStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
        }

        val step: MongoDbSaveStepSpecificationImpl<*> =
            previousStep.nextSteps[0] as MongoDbSaveStepSpecificationImpl<*>

        val database = step.queryConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("database")
        assertThat(database(relaxedMockk(), relaxedMockk())).isEqualTo("db")

        val collection =
            step.queryConfig.getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> String>("collection")
        assertThat(collection(relaxedMockk(), relaxedMockk())).isEqualTo("col")
    }
}
