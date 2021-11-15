package io.qalipsis.plugins.mondodb.save

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import org.bson.Document


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform inserts into MongoDB.
 *
 * @property mongoDbSaveQueryClient client to use to execute the io.qalipsis.plugins.mondodb.save for the current step.
 * @property databaseName closure to generate the string for the database name.
 * @property collectionName closure to generate the string for the collection name.
 * @property recordsFactory closure to generate a list of [Document].
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSaveStep<I>(
    id: StepId,
    retryPolicy: RetryPolicy?,
    private val mongoDbSaveQueryClient: MongoDbSaveQueryClient,
    private val databaseName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val collectionName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val recordsFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<Document>)
) : AbstractStep<I, MongoDBSaveResult<I>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        mongoDbSaveQueryClient.start()
    }

    override suspend fun execute(context: StepContext<I, MongoDBSaveResult<I>>) {
        val input = context.receive()
        val database = databaseName(context, input)
        val collection = collectionName(context, input)
        val records = recordsFactory(context, input)

        val metrics = mongoDbSaveQueryClient.execute(database, collection, records, context.toEventTags())

        context.send(MongoDBSaveResult(input, metrics))
    }

    override suspend fun stop(context: StepStartStopContext) {
        mongoDbSaveQueryClient.stop()
    }
}
