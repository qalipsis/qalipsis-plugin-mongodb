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


