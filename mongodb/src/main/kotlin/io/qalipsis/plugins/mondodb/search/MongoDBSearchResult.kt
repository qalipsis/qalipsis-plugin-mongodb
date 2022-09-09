package io.qalipsis.plugins.mondodb.search

import io.qalipsis.plugins.mondodb.MongoDbQueryMeters
import org.bson.Document

/**
 * A wrapper for meters and documents.
 *
 * @property input the input value used to generate the search query
 * @property documents result of search query procedure in MongoDB
 * @property meters meters of the query
 *
 * @author Eric Jessé
 */
class MongoDBSearchResult<I>(
    val input: I,
    val documents: List<Document>,
    val meters: MongoDbQueryMeters
)
