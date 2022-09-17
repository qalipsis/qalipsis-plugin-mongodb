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

package io.qalipsis.plugins.mondodb.search

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.mondodb.Sorting
import org.bson.conversions.Bson


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform any kind of query to get records from MongoDB.
 *
 * @property mongoDbQueryClient client to use to execute the io.qalipsis.plugins.mondodb.search.search for the current step
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
