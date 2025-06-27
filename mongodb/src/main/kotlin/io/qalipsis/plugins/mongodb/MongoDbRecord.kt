/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.mongodb

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