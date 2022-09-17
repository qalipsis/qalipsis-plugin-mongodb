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

package io.qalipsis.plugins.mongodb.poll

import io.qalipsis.plugins.mongodb.MongoDbQueryMeters
import io.qalipsis.plugins.mongodb.MongoDbRecord

/**
 * Wrapper for the result of poll in MongoDB.
 *
 *
 * @property records list of MongoDB records.
 * @property meters of the poll step.
 *
 * @author Carlos Vieira
 */
class MongoDBPollResults(
    val records: List<MongoDbRecord>,
    val meters: MongoDbQueryMeters
) : Iterable<MongoDbRecord> {

    override fun iterator(): Iterator<MongoDbRecord> {
        return records.iterator()
    }
}
