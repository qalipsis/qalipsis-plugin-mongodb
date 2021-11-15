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
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbDocumentPollSingleConverterTest {

    private val converter = MongoDbDocumentPollSingleConverter("db", "col")

    @Test
    @Timeout(5)
    internal fun `should deserialize and count the records`() = runBlockingTest {
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
