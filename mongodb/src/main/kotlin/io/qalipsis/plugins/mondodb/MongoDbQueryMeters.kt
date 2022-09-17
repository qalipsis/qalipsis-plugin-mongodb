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

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property fetchedRecords count of received records
 * @property fetchedBytes total count of received bytes
 * @property timeToResult time to until the complete successful response
 *
 * @author Eric Jessé
 */
data class MongoDbQueryMeters(
    val fetchedRecords: Int,
    val timeToResult: Duration
)
