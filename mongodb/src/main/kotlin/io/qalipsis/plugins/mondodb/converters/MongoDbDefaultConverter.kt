package io.qalipsis.plugins.mondodb.converters

import io.qalipsis.plugins.mondodb.MongoDbRecord
import org.bson.Document
import java.util.concurrent.atomic.AtomicLong

/**
 * Default converter of MongoDB document.
 *
 * @author Alexander Sosnovsky
 */
internal abstract class MongoDbDefaultConverter {
    protected fun convert(offset: AtomicLong, documents: List<Document>, databaseName: String, collectionName: String): List<MongoDbRecord> {
        return documents.map { document ->
            MongoDbRecord(
                offset = offset.getAndIncrement(),
                record = document,
                database = databaseName,
                collection = collectionName
            )
        }
    }
}

