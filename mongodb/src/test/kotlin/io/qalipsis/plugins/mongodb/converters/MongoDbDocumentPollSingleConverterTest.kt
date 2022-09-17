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

package io.qalipsis.plugins.cassandra.converters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import assertk.assertions.prop
import io.mockk.coJustRun
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import io.qalipsis.plugins.mondodb.MongoDbRecord
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentPollSingleConverter
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
internal class MongoDbDocumentPollSingleConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val converter = MongoDbDocumentPollSingleConverter("db", "col")

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
        val resultCaptor = mutableListOf<MongoDbRecord>()

        //when
        converter.supply(
            AtomicLong(0),
            queryResults,
            relaxedMockk { coJustRun { send(capture(resultCaptor)) } }
        )

        //then
        assertThat(resultCaptor).all {
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
