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

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.gte
import io.mockk.spyk
import io.qalipsis.plugins.mongodb.Sorting
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.Document
import org.junit.Test

internal class MongoDbPollStatementTest {

    @Test
    fun `should fail when there is no sort clause`() {
        assertFailure {
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                // given
                sortClauseValues = linkedMapOf(),
                tieBreakerName = "any"
            )
        }.hasMessage("The provided query has no sort clause")
    }

    @Test
    fun `should fail when there tie-breaker is not the first column in sort clause values`() {
        assertFailure {
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                // given
                sortClauseValues = linkedMapOf("one" to Sorting.ASC, "two" to Sorting.DESC),
                // given
                tieBreakerName = "two"
            )
        }.hasMessage("The tie-breaker should be set as the first sorting column")
    }

    @Test
    fun `should not have tie-breaker before the first request`() {
        // given
        val pollStatement = MongoDbPollStatement(
            databaseName = "any",
            collectionName = "any",
            findClause = Document(),
            sortClauseValues = linkedMapOf("one" to Sorting.ASC),
            tieBreakerName = "one"
        )

        // when only initialization happens

        // then
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should reset() clean up tie-breaker`() {
        // given
        val pollStatement = spyk(
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                sortClauseValues = linkedMapOf("one" to Sorting.ASC),
                tieBreakerName = "one"
            )
        )

        // when (minor check)
        pollStatement.saveTieBreakerValueForNextPoll(Document().append("one", "the value"))

        // then  (minor check)
        assertThat(pollStatement).typedProp<BsonValue>("tieBreaker").isEqualTo(BsonString("the value"))

        // when (major check)
        pollStatement.reset()

        // then (major check)
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should return original find clause for the first request`() {
        // given
        val initialFindClause = and(gte("field", "some value"))
        val pollStatement = spyk(
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = initialFindClause,
                sortClauseValues = linkedMapOf("one" to Sorting.ASC),
                tieBreakerName = "one"
            )
        )

        // when
        val filterForFirstRequest = pollStatement.filter

        // then
        assertThat(filterForFirstRequest).isEqualTo(initialFindClause)
    }

    @Test
    fun `should return proper bson for sorting clause`() {
        // given
        val pollStatement = spyk(
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                sortClauseValues = linkedMapOf("asc" to Sorting.ASC, "desc" to Sorting.DESC),
                tieBreakerName = "asc"
            )
        )

        // then
        val actualSort = pollStatement.sorting
        val expectedSort = Document("asc", 1).append("desc", -1)
        assertThat(actualSort).isEqualTo(expectedSort)
    }
}
