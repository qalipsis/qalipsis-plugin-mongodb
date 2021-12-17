package io.qalipsis.plugins.mondodb.save

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.mondodb.MongoDbStepSpecification
import org.bson.Document

/**
 * Specification for a [io.qalipsis.plugins.mondodb.save.MongoDbSaveStep] to save data to a MongoDB.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbSaveStepSpecification<I> :
    StepSpecification<I, I, MongoDbSaveStepSpecification<I>>,
    MongoDbStepSpecification<I, I, MongoDbSaveStepSpecification<I>> {

    /**
     * Configures the connection to the MongoDb server.
     */
    fun connect(clientBuilder: () -> com.mongodb.reactivestreams.client.MongoClient)

    /**
     * Defines the statement to execute when saving.
     */
    fun query(queryConfiguration: MongoDbSaveQueryConfiguration<I>.() -> Unit)

    /**
     * Configures the monitoring of the save step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)

}

/**
 * Implementation of [MongoDbSaveStepSpecification].
 *
 * @author Alexander Sosnovsky
 */
@Spec
internal class MongoDbSaveStepSpecificationImpl<I> :
    MongoDbSaveStepSpecification<I>,
    AbstractStepSpecification<I, I, MongoDbSaveStepSpecification<I>>() {

    internal var clientBuilder: (() -> com.mongodb.reactivestreams.client.MongoClient) = { MongoClients.create() }

    internal var queryConfig = MongoDbSaveQueryConfiguration<I>()

    internal var monitoringConfig = StepMonitoringConfiguration()

    override fun connect(clientBuilder: () -> com.mongodb.reactivestreams.client.MongoClient) {
        this.clientBuilder = clientBuilder
    }

    override fun query(queryConfiguration: MongoDbSaveQueryConfiguration<I>.() -> Unit) {
        queryConfig.queryConfiguration()
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }
}

/**
 * Configuration of routing and generation of documents to save in MongoDB.
 *
 * @property database closure to generate the string for the database name
 * @property collection closure to generate the string for the collection name
 * @property records closure to generate a list of [Document]
 *
 * @author Alexander Sosnovsky
 */
@Spec
data class MongoDbSaveQueryConfiguration<I>(
    internal var database: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var collection: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var records: suspend (ctx: StepContext<*, *>, input: I) -> List<Document> = { _, _ -> listOf() }
)

/**
 * Saves documents into MongoD.
 *
 * @author Alexander Sosnovsky
 */
fun <I> MongoDbStepSpecification<*, I, *>.save(
    configurationBlock: MongoDbSaveStepSpecification<I>.() -> Unit
): MongoDbSaveStepSpecification<I> {
    val step = MongoDbSaveStepSpecificationImpl<I>()
    step.configurationBlock()

    this.add(step)
    return step
}
