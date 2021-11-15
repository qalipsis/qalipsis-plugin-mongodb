package io.qalipsis.plugins.mondodb

import org.bson.Document

/**
 * Qalipsis representation of a fetched MongoDB document.
 *
 * @author Alexander Sosnovsky
 *
 * @property value [Map] of value from [Document]
 * @property id special identifier from db
 * @property source name of db and collection (dbname.colname example) from data were received
 * @property offset record offset as provided by MongoDb
 * @property receivingInstant received timestamp as provided by MongoDb
 */
data class MongoDbRecord(
    val value: Map<String, Any?>,
    val id: Any,
    val source: String,
    val offset: Long,
    val receivingInstant: Long = System.currentTimeMillis(),
) {
    internal constructor(
        offset: Long, record: Document, database: String, collection: String, idField: String? = "_id"
    ) : this(
        value = record.toMap(),
        id = record.getObjectId(idField),
        source = "$database.$collection",
        offset = offset
    )
}