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

package io.qalipsis.plugins.mongodb.search

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.mongodb.MongoDbStepSpecification
import io.qalipsis.plugins.mongodb.Sorting
import org.bson.Document

/**
 * Specification for a [io.qalipsis.plugins.mongodb.search.MongoDbSearchStep] to search data from a MongoDB.
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
 * Searches data in MongoDB.
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
