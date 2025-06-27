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
