package io.qalipsis.plugins.mondodb.save

import io.qalipsis.api.context.StepStartStopContext
import org.bson.Document


/**
 * Client to save records to MongoDb.
 *
 * @author Alexander Sosnovsky
 */
internal interface MongoDbSaveQueryClient {

    /**
     * Initializes the client and connects to the MongoDB server.
     */
    suspend fun start(context: StepStartStopContext)

    /**
     * Inserts records to the MongoDB server.
     */
    suspend fun execute(
        dbName: String, collName: String, records: List<Document>,
        contextEventTags: Map<String, String>
    ): MongoDbSaveQueryMeters

    /**
     * Cleans the client and closes the connections to the MongoDB server.
     */
    suspend fun stop(context: StepStartStopContext)
}
