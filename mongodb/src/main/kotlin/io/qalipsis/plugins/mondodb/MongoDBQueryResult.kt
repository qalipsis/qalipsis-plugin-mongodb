package io.qalipsis.plugins.mondodb

import org.bson.Document

/**
 * A wrapper for meters and documents.
 *
 * @property documents result of search query procedure in MongoDB
 * @property meters meters of the query
 *
 * @author Carlos Vieira
 */
class MongoDBQueryResult(
    val documents: List<Document>,
    val meters: MongoDbQueryMeters
)
