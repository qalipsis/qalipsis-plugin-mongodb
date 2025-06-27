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
