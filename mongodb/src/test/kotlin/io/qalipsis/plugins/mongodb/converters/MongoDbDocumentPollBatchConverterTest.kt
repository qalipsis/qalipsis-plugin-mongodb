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

package io.qalipsis.plugins.cassandra.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isSameAs
import assertk.assertions.key
import assertk.assertions.prop
import io.mockk.coJustRun
import io.mockk.slot
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.plugins.mongodb.MongoDbQueryMeters
import io.qalipsis.plugins.mongodb.MongoDbRecord
import io.qalipsis.plugins.mongodb.converters.MongoDbDocumentPollBatchConverter
import io.qalipsis.plugins.mongodb.poll.MongoDBPollResults
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbDocumentPollBatchConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val converter = MongoDbDocumentPollBatchConverter("db", "col")

    @Test
    @Timeout(5)
    internal fun `should deserialize and count the records`() = testDispatcherProvider.runTest {
        //given
        val document1 = Document("_id", ObjectId()).append("status", true).append("name", "name1")
        val document2 = Document("_id", ObjectId()).append("status", false).append("name", "name2")
        val queryResults = MongoDBQueryResult(
            documents = listOf(
                document1,
                document2,
            ),
            meters = MongoDbQueryMeters(
                2,
                Duration.ofMillis(123)
            )
        )
        val resultCaptor = slot<MongoDBPollResults>()

        //when
        converter.supply(
            AtomicLong(0),
            queryResults,
            relaxedMockk { coJustRun { send(capture(resultCaptor)) } }
        )

        //then
        assertThat(resultCaptor.captured).all {
            prop(MongoDBPollResults::meters).isSameAs(queryResults.meters)
            prop(MongoDBPollResults::records).all {
                hasSize(2)
                index(0).all {
                    prop(MongoDbRecord::offset).isEqualTo(0)
                    prop(MongoDbRecord::source).isEqualTo("db.col")
                    prop(MongoDbRecord::value).all {
                        hasSize(3)
                        key("_id").isEqualTo(document1.getObjectId("_id"))
                        key("status").isEqualTo(true)
                        key("name").isEqualTo("name1")
                    }
                }
                index(1).all {
                    prop(MongoDbRecord::offset).isEqualTo(1)
                    prop(MongoDbRecord::source).isEqualTo("db.col")
                    prop(MongoDbRecord::value).all {
                        hasSize(3)
                        key("_id").isEqualTo(document2.getObjectId("_id"))
                        key("status").isEqualTo(false)
                        key("name").isEqualTo("name2")
                    }
                }
            }
        }
    }
}
