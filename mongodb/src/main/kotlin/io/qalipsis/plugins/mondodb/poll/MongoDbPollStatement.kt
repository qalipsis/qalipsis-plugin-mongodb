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

package io.qalipsis.plugins.mondodb.poll

import io.qalipsis.plugins.mondodb.Sorting
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson

/**
 * MongoDb statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @property databaseName - a name of a database
 * @property collectionName - a name of a collection
 * @property findClause - initial find clause for the first request
 * @property sortClauseValues - a map with field name as a key and ordering as a value
 * @property tieBreakerName - tie breaker name
 * @property filter - a constructed find clause as BSON that contains a tie-breaker for the second and subsequent calls
 * @property sorting - a constructed sort clause as BSON based on [sortClauseValues]
 * @author Maxim Golokhov
 */
internal class MongoDbPollStatement(
    val databaseName: String,
    val collectionName: String,
    private val findClause: Bson,
    private val sortClauseValues: LinkedHashMap<String, Sorting>,
    private val tieBreakerName: String,
) : PollStatement {

    private var tieBreaker: BsonValue? = null

    private val findClauseAsBson = findClause.toBsonDocument()

    init {
        validateSortingClauseIsNotEmpty()
        validateTieBreakerIsFirstSortingColumn()
    }

    private fun validateTieBreakerIsFirstSortingColumn() {
        if (tieBreakerName != sortClauseValues.keys.first()) {
            throw IllegalArgumentException("The tie-breaker should be set as the first sorting column")
        }
    }

    private fun validateSortingClauseIsNotEmpty() {
        if (sortClauseValues.isEmpty()) {
            throw IllegalArgumentException("The provided query has no sort clause")
        }
    }

    override fun saveTieBreakerValueForNextPoll(document: Document) {
        tieBreaker = document.toBsonDocument()[tieBreakerName]
    }

    override fun reset() {
        tieBreaker = null
    }

    override val filter: Bson
        get() {
            return tieBreaker?.let {
                val result = findClause.toBsonDocument()
                result.append(tieBreakerName, it)
                result
            } ?: findClauseAsBson
        }

    override val sorting: Bson by lazy(LazyThreadSafetyMode.NONE) {
        val result = Document()
        sortClauseValues.forEach { (name, order) ->
            if (Sorting.ASC == order) {
                result.append(name, 1)
            } else if (Sorting.DESC == order) {
                result.append(name, -1)
            }
        }
        result
    }
}
