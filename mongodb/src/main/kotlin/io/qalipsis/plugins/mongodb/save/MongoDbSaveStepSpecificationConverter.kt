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

package io.qalipsis.plugins.mongodb.save

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import org.bson.Document

/**
 * [StepSpecificationConverter] from [MongoDbSaveStepSpecificationImpl] to [MongoDbSaveStep]
 * to use the Save API.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class MongoDbSaveStepSpecificationConverter(
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<MongoDbSaveStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is MongoDbSaveStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<MongoDbSaveStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name

        @Suppress("UNCHECKED_CAST")
        val step = MongoDbSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            mongoDbSaveQueryClient = MongoDbSaveQueryClientImpl(
                spec.clientBuilder,
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),
            databaseName = spec.queryConfig.database as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            collectionName = spec.queryConfig.collection as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            recordsFactory = spec.queryConfig.documents as suspend (ctx: StepContext<*, *>, input: I) -> List<Document>,
        )
        creationContext.createdStep(step)
    }

}
