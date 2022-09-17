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

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import org.bson.Document


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform inserts into MongoDB.
 *
 * @property mongoDbSaveQueryClient client to use to execute the [save] for the current step.
 * @property databaseName closure to generate the string for the database name.
 * @property collectionName closure to generate the string for the collection name.
 * @property recordsFactory closure to generate a list of [Document].
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSaveStep<I>(
    id: StepName,
    retryPolicy: RetryPolicy?,
    private val mongoDbSaveQueryClient: MongoDbSaveQueryClient,
    private val databaseName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val collectionName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val recordsFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<Document>)
) : AbstractStep<I, MongoDBSaveResult<I>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        mongoDbSaveQueryClient.start(context)
    }

    override suspend fun execute(context: StepContext<I, MongoDBSaveResult<I>>) {
        val input = context.receive()
        val database = databaseName(context, input)
        val collection = collectionName(context, input)
        val records = recordsFactory(context, input)

        val metrics = mongoDbSaveQueryClient.execute(database, collection, records, context.toEventTags())

        context.send(MongoDBSaveResult(input, metrics))
    }

    override suspend fun stop(context: StepStartStopContext) {
        mongoDbSaveQueryClient.stop(context)
    }
}
