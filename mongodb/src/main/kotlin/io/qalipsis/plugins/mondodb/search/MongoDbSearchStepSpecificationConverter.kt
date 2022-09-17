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

package io.qalipsis.plugins.mondodb.search

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.mondodb.Sorting
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import org.bson.Document

/**
 * [StepSpecificationConverter] from [MongoDbSearchStepSpecificationImpl] to [MongoDbSearchStep]
 * to use the Search API.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class MongoDbSearchStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<MongoDbSearchStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is MongoDbSearchStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<MongoDbSearchStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name

        @Suppress("UNCHECKED_CAST")
        val step = MongoDbSearchStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            mongoDbQueryClient = MongoDbQueryClientImpl(
                ioCoroutineScope,
                spec.clientFactory,
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),

            databaseName = spec.searchConfig.database as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            collectionName = spec.searchConfig.collection as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            filter = spec.searchConfig.query as suspend (ctx: StepContext<*, *>, input: Any?) -> Document,
            sorting = spec.searchConfig.sort as suspend (ctx: StepContext<*, *>, input: Any?) -> LinkedHashMap<String, Sorting>
        )
        creationContext.createdStep(step)
    }

}
