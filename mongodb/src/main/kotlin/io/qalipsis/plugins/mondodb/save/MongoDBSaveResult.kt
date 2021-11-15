package io.qalipsis.plugins.mondodb.save

/**
 * Wrapper for the result of save records procedure in MongoDB.
 *
 * @property input the data to save in MongoDb
 * @property meters meters of the save step
 *
 * @author Carlos Vieira
 */
internal class MongoDBSaveResult<I>(
    val input: I,
    val meters: MongoDbSaveQueryMeters
)
