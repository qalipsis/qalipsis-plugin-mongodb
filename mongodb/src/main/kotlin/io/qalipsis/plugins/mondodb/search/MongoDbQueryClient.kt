package io.qalipsis.plugins.mondodb.search

import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.Sorting
import org.bson.conversions.Bson


/**
 * Client to query from MongoDb.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbQueryClient {

    /**
     * Initializes the client and connects to the MongoDB server.
     */
    suspend fun init()

    /**
     * Executes a query and returns the list of results.
     */
    suspend fun execute(
        database: String,
        collection: String,
        findClause: Bson,
        sorting: LinkedHashMap<String, Sorting>,
        contextEventTags: Map<String, String>
    ): MongoDBQueryResult

    /**
     * Cleans the client and closes the connections to the MongoDB server.
     */
    suspend fun stop()
}


