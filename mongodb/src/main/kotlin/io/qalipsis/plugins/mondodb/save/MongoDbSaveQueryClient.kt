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

package io.qalipsis.plugins.mondodb.save

import io.qalipsis.api.context.StepStartStopContext
import org.bson.Document


/**
 * Client to save records to MongoDb.
 *
 * @author Alexander Sosnovsky
 */
internal interface MongoDbSaveQueryClient {

    /**
     * Initializes the client and connects to the MongoDB server.
     */
    suspend fun start(context: StepStartStopContext)

    /**
     * Inserts records to the MongoDB server.
     */
    suspend fun execute(
        dbName: String, collName: String, records: List<Document>,
        contextEventTags: Map<String, String>
    ): MongoDbSaveQueryMeters

    /**
     * Cleans the client and closes the connections to the MongoDB server.
     */
    suspend fun stop(context: StepStartStopContext)
}
