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
