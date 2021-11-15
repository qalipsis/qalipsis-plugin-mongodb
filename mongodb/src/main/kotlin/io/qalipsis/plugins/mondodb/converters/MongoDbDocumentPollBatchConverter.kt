package io.qalipsis.plugins.mondodb.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.poll.MongoDBPollResults
import io.qalipsis.plugins.mondodb.search.MongoDBSearchResults
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of MongoDb documents and forwards it
 * as a list of [MongoDBSearchResults].
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbDocumentPollBatchConverter(
    private val databaseName: String,
    private val collectionName: String,
) : DatasourceObjectConverter<MongoDBQueryResult, MongoDBPollResults>, MongoDbDefaultConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: MongoDBQueryResult,
        output: StepOutput<MongoDBPollResults>
    ) {
        tryAndLogOrNull(log) {
            output.send(
                MongoDBPollResults(
                    records = convert(offset, value.documents, databaseName, collectionName),
                    meters = value.meters
                )
            )
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
