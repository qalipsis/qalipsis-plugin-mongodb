package io.qalipsis.plugins.mondodb.search

import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import io.qalipsis.plugins.mondodb.MongoDbRecord

/**
 *  Wrapper for the result of batch search query procedure in MongoDB.
 *
 * @property input input for the search step
 * @property records list of MongoDB records retrieved from DB
 * @property meters meters of the search step
 *
 * @author Carlos Vieira
 */
class MongoDBSearchResults<I>(
    val input: I,
    val records: List<MongoDbRecord>,
    val meters: MongoDbQueryMeters
) : Iterable<MongoDbRecord> {

    override fun iterator(): Iterator<MongoDbRecord> {
        return records.iterator()
    }
}
