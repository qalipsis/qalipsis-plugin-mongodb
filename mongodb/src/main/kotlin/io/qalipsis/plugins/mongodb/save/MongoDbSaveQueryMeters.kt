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

package io.qalipsis.plugins.mongodb.save

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property savedRecords count of saved records
 * @property savedBytes total bytes saved
 * @property timeToResult time to until the confirmation of the successful response
 *
 * @author Eric Jessé
 */
data class MongoDbSaveQueryMeters(
    val savedRecords: Int,
    val failedRecords: Int,
    val timeToResult: Duration
)
