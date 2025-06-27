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

package io.qalipsis.plugins.mongodb.poll

import io.qalipsis.plugins.mongodb.Sorting
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
