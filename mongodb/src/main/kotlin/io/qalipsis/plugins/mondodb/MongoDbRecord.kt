/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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