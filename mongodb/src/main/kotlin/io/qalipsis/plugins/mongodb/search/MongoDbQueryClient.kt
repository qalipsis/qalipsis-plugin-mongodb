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

import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.plugins.mongodb.Sorting
import org.bson.conversions.Bson


/**
 * Client to query from MongoDb.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbQueryClient {

    /**
     * Initializes the client and connects to the MongoDB server.
     */
    suspend fun init()

    /**
     * Executes a query and returns the list of results.
     */
    suspend fun execute(
        database: String,
        collection: String,
        findClause: Bson,
        sorting: LinkedHashMap<String, Sorting>,
        contextEventTags: Map<String, String>
    ): MongoDBQueryResult

    /**
     * Initiate the meters if they are enabled.
     */
    suspend fun start(context: StepStartStopContext)

    /**
     * Cleans the client and closes the connections to the MongoDB server.
     */
    suspend fun stop(context: StepStartStopContext)
}


