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

package io.qalipsis.plugins.mondodb.converters;

import io.qalipsis.api.context.StepOutput
import java.util.concurrent.atomic.AtomicLong

interface MongoDbDocumentConverter<R, O, I> {
    /**
     * Sends [value] to the [output] channel in any form.
     *
     * @param offset an reference to the offset, it is up to the implementation to increment it
     * @param value input value to send after any conversion to the output
     * @param input received in the channel to be send along with the data after conversion
     * @param output channel to received the data after conversion
     */
    suspend fun supply(
        offset: AtomicLong,
        value: R,
        databaseName: String,
        collectionName: String,
        input: I,
        output: StepOutput<O>
    )
}
