package io.qalipsis.plugins.mondodb.poll

import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import io.qalipsis.plugins.mondodb.MongoDbRecord

/**
 * Wrapper for the result of poll in MongoDB.
 *
 *
 * @property records list of MongoDB records.
 * @property meters of the poll step.
 *
 * @author Carlos Vieira
 */
class MongoDBPollResults(
    val records: List<MongoDbRecord>,
    val meters: MongoDbQueryMeters
) : Iterable<MongoDbRecord> {

    override fun iterator(): Iterator<MongoDbRecord> {
        return records.iterator()
    }
}
