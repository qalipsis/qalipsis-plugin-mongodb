package io.qalipsis.plugins.mondodb.search

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.mondodb.MongoDbStepSpecification
import io.qalipsis.plugins.mondodb.Sorting
import org.bson.Document

/**
 * Specification for a [io.qalipsis.plugins.mondodb.search.MongoDbSearchStep] to search data from a MongoDB.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbSearchStepSpecification<I> :
    StepSpecification<I, MongoDBSearchResult<I>, MongoDbSearchStepSpecification<I>>,
    ConfigurableStepSpecification<I, MongoDBSearchResult<I>, MongoDbSearchStepSpecification<I>>,
    MongoDbStepSpecification<I, MongoDBSearchResult<I>, MongoDbSearchStepSpecification<I>> {

    /**
     * Configures the connection to the MongoDb server.
     */
    fun connect(clientFactory: () -> com.mongodb.reactivestreams.client.MongoClient)

    /**
     * Defines the statement to execute when searching. The query must contain ordering clauses.
     */
    fun search(searchConfiguration: MongoDbQueryConfiguration<I>.() -> Unit)

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
    AbstractStepSpecification<I, MongoDBSearchResult<I>, MongoDbSearchStepSpecification<I>>() {

    internal var clientFactory: (() -> com.mongodb.reactivestreams.client.MongoClient) = { MongoClients.create() }

    internal var searchConfig = MongoDbQueryConfiguration<I>()

    internal var monitoringConfig = StepMonitoringConfiguration()

    override fun connect(clientFactory: () -> com.mongodb.reactivestreams.client.MongoClient) {
        this.clientFactory = clientFactory
    }

    override fun search(searchConfiguration: MongoDbQueryConfiguration<I>.() -> Unit) {
        searchConfig.searchConfiguration()
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
data class MongoDbQueryConfiguration<I> internal constructor(
    internal var database: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var collection: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var query: suspend (ctx: StepContext<*, *>, input: I) -> Document = { _, _ -> Document() },
    internal var sort: suspend (ctx: StepContext<*, *>, input: I) -> LinkedHashMap<String, Sorting> = { _, _ -> linkedMapOf() }
) {
    /**
     * Build the name of the database to save the data in.
     */
    fun database(databaseFactory: suspend (ctx: StepContext<*, *>, input: I) -> String) {
        database = databaseFactory
    }

    /**
     * Build the name of the collection to save the data in.
     */
    fun collection(collectionFactory: suspend (ctx: StepContext<*, *>, input: I) -> String) {
        collection = collectionFactory
    }

    /**
     * Build the query to execute.
     */
    fun query(queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> Document) {
        query = queryFactory
    }

    /**
     * Build the ordering strategy of the query to execute.
     */
    fun sort(sortFactory: suspend (ctx: StepContext<*, *>, input: I) -> LinkedHashMap<String, Sorting>) {
        sort = sortFactory
    }
}

/**
 * Searches data in MongoDB using a io.qalipsis.plugins.mondodb.search.search query.
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
