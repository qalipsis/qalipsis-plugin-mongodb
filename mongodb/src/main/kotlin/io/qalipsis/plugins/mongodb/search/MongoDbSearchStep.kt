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

package io.qalipsis.plugins.mongodb.search

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.mongodb.Sorting
import org.bson.conversions.Bson


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform any kind of query to get records from MongoDB.
 *
 * @property mongoDbQueryClient client to use to execute the [search] for the current step
 * @property converter used to return to results to the output channel
 * @property databaseName closure to generate the string for the database name
 * @property collectionName closure to generate the string for the collection name
 * @property filter closure to generate the Bson for the query
 * @property sorting closure to generate a map for the ordering clause
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSearchStep<I>(
    id: StepName,
    retryPolicy: RetryPolicy?,
    private val mongoDbQueryClient: MongoDbQueryClient,
    private val databaseName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val collectionName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val filter: (suspend (ctx: StepContext<*, *>, input: I) -> Bson),
    private val sorting: (suspend (ctx: StepContext<*, *>, input: I) -> LinkedHashMap<String, Sorting>)
) : AbstractStep<I, MongoDBSearchResult<I>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        mongoDbQueryClient.start(context)
    }

    override suspend fun execute(context: StepContext<I, MongoDBSearchResult<I>>) {
        val input = context.receive()
        val database = databaseName(context, input)
        val collection = collectionName(context, input)
        val findClause = filter(context, input)
        val sort = sorting(context, input)
        val results = mongoDbQueryClient.execute(database, collection, findClause, sort, context.toEventTags())
        context.send(MongoDBSearchResult(input, results.documents, results.meters))
    }

    override suspend fun stop(context: StepStartStopContext) {
        mongoDbQueryClient.stop(context)
    }
}
