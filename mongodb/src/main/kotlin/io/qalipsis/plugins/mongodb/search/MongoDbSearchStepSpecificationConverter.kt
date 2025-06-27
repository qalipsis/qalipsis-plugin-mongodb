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

package io.qalipsis.plugins.mongodb.search

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.mongodb.Sorting
import org.bson.Document

/**
 * [StepSpecificationConverter] from [MongoDbSearchStepSpecificationImpl] to [MongoDbSearchStep]
 * to use the Search API.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class MongoDbSearchStepSpecificationConverter(
    private val meterRegistry: CampaignMeterRegistry,
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
