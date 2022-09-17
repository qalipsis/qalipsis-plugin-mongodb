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

package io.qalipsis.plugins.mongodb.save

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.mongodb.MongoDbStepSpecification
import org.bson.Document

/**
 * Specification for a [io.qalipsis.plugins.mongodb.save.MongoDbSaveStep] to save data to a MongoDB.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbSaveStepSpecification<I> :
    StepSpecification<I, MongoDBSaveResult<I>, MongoDbSaveStepSpecification<I>>,
    ConfigurableStepSpecification<I, MongoDBSaveResult<I>, MongoDbSaveStepSpecification<I>>,
    MongoDbStepSpecification<I, MongoDBSaveResult<I>, MongoDbSaveStepSpecification<I>> {

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
    AbstractStepSpecification<I, MongoDBSaveResult<I>, MongoDbSaveStepSpecification<I>>() {

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
 * @property documents closure to generate a list of [Document]
 *
 * @author Alexander Sosnovsky
 */
@Spec
data class MongoDbSaveQueryConfiguration<I> internal constructor(
    internal var database: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var collection: suspend (ctx: StepContext<*, *>, input: I) -> String = { _, _ -> "" },
    internal var documents: suspend (ctx: StepContext<*, *>, input: I) -> List<Document> = { _, _ -> emptyList<Document>() }
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
     * Build the documents to save.
     */
    fun documents(documentsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Document>) {
        documents = documentsFactory
    }
}

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
