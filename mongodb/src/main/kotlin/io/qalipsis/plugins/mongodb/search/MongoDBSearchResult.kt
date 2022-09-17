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

import io.qalipsis.plugins.mongodb.MongoDbQueryMeters
import org.bson.Document

/**
 * A wrapper for meters and documents.
 *
 * @property input the input value used to generate the search query
 * @property documents result of search query procedure in MongoDB
 * @property meters meters of the query
 *
 * @author Eric Jessé
 */
class MongoDBSearchResult<I>(
    val input: I,
    val documents: List<Document>,
    val meters: MongoDbQueryMeters
)
