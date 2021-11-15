package io.qalipsis.plugins.mondodb.search

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import io.qalipsis.plugins.mondodb.MongoDbRecord
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentConverter
import org.bson.Document
import org.bson.conversions.Bson
import java.util.concurrent.atomic.AtomicLong


/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to perform any kind of query to get records from MongoDB.
 *
 * @property mongoDbQueryClient client to use to execute the io.qalipsis.plugins.mondodb.search for the current step
 * @property converter used to return to results to the output channel
 * @property databaseName closure to generate the string for the database name
 * @property collectionName closure to generate the string for the collection name
 * @property filter closure to generate the Bson for the query
 * @property sorting closure to generate a map for the ordering clause
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbSearchStep<I>(
    id: StepId,
    retryPolicy: RetryPolicy?,
    private val mongoDbQueryClient: MongoDbQueryClient,
    private val databaseName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val collectionName: (suspend (ctx: StepContext<*, *>, input: I) -> String),
    private val filter: (suspend (ctx: StepContext<*, *>, input: I) -> Bson),
    private val sorting: (suspend (ctx: StepContext<*, *>, input: I) -> LinkedHashMap<String, Sorting>),
    private val converter: MongoDbDocumentConverter<List<Document>, Any?, I>
) : AbstractStep<I, Pair<I, List<MongoDbRecord>>>(id, retryPolicy) {

    override suspend fun start(context: StepStartStopContext) {
        mongoDbQueryClient.init()
    }

    override suspend fun execute(context: StepContext<I, Pair<I, List<MongoDbRecord>>>) {
        val input = context.receive()

        val rowIndex = AtomicLong()

        val database = databaseName(context, input)
        val collection = collectionName(context, input)
        val findClause = filter(context, input)
        val sort = sorting(context, input)

        val results = mongoDbQueryClient.execute(database, collection, findClause, sort, context.toEventTags())

        @Suppress("UNCHECKED_CAST")
        converter.supply(rowIndex, results.documents, database, collection, input, context as StepOutput<Any?>)
    }

    override suspend fun stop(context: StepStartStopContext) {
        mongoDbQueryClient.stop()
    }
}
