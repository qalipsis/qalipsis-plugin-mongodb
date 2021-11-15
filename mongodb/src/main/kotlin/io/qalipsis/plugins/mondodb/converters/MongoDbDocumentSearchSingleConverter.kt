package io.qalipsis.plugins.mondodb.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbRecord
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of MongoDb documents and forwards each of
 * them converted to a [MongoDbRecord].
 *
 * @author Alexander Sosnovsky
 */
internal class MongoDbDocumentSearchSingleConverter<I> :
    MongoDbDocumentConverter<MongoDBQueryResult, MongoDbRecord, I>, MongoDbDefaultConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: MongoDBQueryResult,
        databaseName: String,
        collectionName: String,
        input: I,
        output: StepOutput<MongoDbRecord>
    ) {
        tryAndLogOrNull(log) {
            convert(offset, value.documents, databaseName, collectionName).forEach {
                output.send(it)
            }
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}


