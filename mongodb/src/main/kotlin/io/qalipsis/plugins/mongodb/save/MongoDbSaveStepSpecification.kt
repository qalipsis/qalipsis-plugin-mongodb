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
