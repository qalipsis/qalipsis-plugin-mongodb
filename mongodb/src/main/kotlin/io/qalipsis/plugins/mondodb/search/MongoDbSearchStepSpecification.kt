package io.qalipsis.plugins.mondodb

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import org.bson.Document

/**
 * Specification for a [io.qalipsis.plugins.mondodb.search.MongoDbSearchStep] to search data from a MongoDB.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbSearchStepSpecification<I> :
    StepSpecification<I, Pair<I, List<MongoDbRecord>>, MongoDbSearchStepSpecification<I>>,
    MongoDbStepSpecification<I, Pair<I, List<MongoDbRecord>>, MongoDbSearchStepSpecification<I>> {

    /**
     * Configures the connection to the MongoDb server.
     */
    fun connect(clientFactory: () -> com.mongodb.reactivestreams.client.MongoClient)

    /**
     * Defines the statement to execute when searching. The query must contain ordering clauses.
     */
    fun search(searchConfiguration: MongoDbQueryConfiguration<I>.() -> Unit)

    /**
     * Returns each record of a batch individually to the next steps.
     */
    fun flatten(): StepSpecification<I, MongoDbRecord, *>

    /**
     * Configures the monitoring of the search step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)
}

/**
 * Implementation of [MongoDbSearchStepSpecification].
 *
 * @author Alexander Sosnovsky
 */
@Spec
internal class MongoDbSearchStepSpecificationImpl<I> :
    MongoDbSearchStepSpecification<I>,
    AbstractStepSpecification<I, Pair<I, List<MongoDbRecord>>, MongoDbSearchStepSpecification<I>>() {

    internal var clientFactory: (() -> com.mongodb.reactivestreams.client.MongoClient) = { MongoClients.create() }

    internal var searchConfig = MongoDbQueryConfiguration<I>()

    internal var monitoringConfig = StepMonitoringConfiguration()

    internal var flattenOutput = false

    override fun connect(clientFactory: () -> com.mongodb.reactivestreams.client.MongoClient) {
        this.clientFactory = clientFactory
    }

    override fun search(searchConfiguration: MongoDbQueryConfiguration<I>.() -> Unit) {
        searchConfig.searchConfiguration()
    }

    override fun flatten(): StepSpecification<I, MongoDbRecord, *> {
        flattenOutput = true

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<I, MongoDbRecord, *>
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }
}

/**
 * @property database closure to generate the string for the database name
 * @property collection closure to generate the string for the collection name
 * @property query closure to generate the Bson for the query
 * @property sort closure to generate a map for the ordering clause
 */
@Spec
data class MongoDbQueryConfiguration<I>(
    internal var database: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var collection: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var query: suspend (ctx: StepContext<*, *>, input: I) -> Document = { _, _ -> Document() },
    internal var sort: suspend (ctx: StepContext<*, *>, input: I) -> LinkedHashMap<String, Sorting> = { _, _ -> linkedMapOf() }
)

/**
 * Searches data in MongoDB using a io.qalipsis.plugins.mondodb.search query.
 *
 * @author Alexander Sosnovsky
 */
fun <I> MongoDbStepSpecification<*, I, *>.search(
    configurationBlock: MongoDbSearchStepSpecification<I>.() -> Unit
): MongoDbSearchStepSpecification<I> {
    val step = MongoDbSearchStepSpecificationImpl<I>()
    step.configurationBlock()
    this.add(step)
    return step
}
