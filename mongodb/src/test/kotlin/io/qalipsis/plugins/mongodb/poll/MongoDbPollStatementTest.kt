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

package io.qalipsis.plugins.mongodb.poll

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isNull
import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.gte
import io.mockk.spyk
import io.qalipsis.plugins.mongodb.Sorting
import io.qalipsis.plugins.mongodb.poll.MongoDbPollStatement
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.Document
import org.junit.Test

internal class MongoDbPollStatementTest {

    @Test
    fun `should fail when there is no sort clause`() {
        assertThat {
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                // given
                sortClauseValues = linkedMapOf(),
                tieBreakerName = "any"
            )
            // when initialisation happens
            // then
        }.isFailure().hasMessage("The provided query has no sort clause")
    }

    @Test
    fun `should fail when there tie-breaker is not the first column in sort clause values`() {
        assertThat {
            MongoDbPollStatement(
                databaseName = "any",
                collectionName = "any",
                findClause = Document(),
                // given
                sortClauseValues = linkedMapOf("one" to Sorting.ASC, "two" to Sorting.DESC),
                // given
                tieBreakerName = "two"
            )
            // when initialisation happens
            // then
        }.isFailure().hasMessage("The tie-breaker should be set as the first sorting column")
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
