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

import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.mongodb.MongoDBQueryResult
import io.qalipsis.plugins.mongodb.converters.MongoDbDocumentPollBatchConverter
import io.qalipsis.plugins.mongodb.converters.MongoDbDocumentPollSingleConverter
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope

/**
 * [StepSpecificationConverter] from [MongoDbPollStepSpecificationImpl] to [MongoDbIterativeReader] for a data source.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class MongoDbPollStepSpecificationConverter(
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val coroutineScope: CoroutineScope
) : StepSpecificationConverter<MongoDbPollStepSpecificationImpl> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is MongoDbPollStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<MongoDbPollStepSpecificationImpl>) {
        val spec = creationContext.stepSpecification
        val pollStatement = buildPollStatement(spec)
        val stepId = spec.name

        val reader = MongoDbIterativeReader(
            coroutineScope = coroutineScope,
            clientBuilder = spec.client,
            pollStatement = pollStatement,
            pollDelay = spec.pollPeriod,
            eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
            meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
        )

        val converter = buildConverter(spec)

        val step = IterativeDatasourceStep(
            stepId,
            reader,
            NoopDatasourceObjectProcessor(),
            converter
        )
        creationContext.createdStep(step)
    }

    fun buildPollStatement(
        spec: MongoDbPollStepSpecificationImpl
    ): MongoDbPollStatement {
        val search = spec.searchConfig
        return MongoDbPollStatement(
            databaseName = search.database,
            collectionName = search.collection,
            findClause = search.query,
            sortClauseValues = search.sort,
            tieBreakerName = search.tieBreaker
        )
    }

    private fun buildConverter(
        spec: MongoDbPollStepSpecificationImpl,
    ): DatasourceObjectConverter<MongoDBQueryResult, out Any> {
        return if (spec.flattenOutput) {
            MongoDbDocumentPollSingleConverter(
                spec.searchConfig.database,
                spec.searchConfig.collection
            )
        } else {
            MongoDbDocumentPollBatchConverter(
                spec.searchConfig.database,
                spec.searchConfig.collection
            )
        }
    }
}
