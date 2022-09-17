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

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
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
    private val meterRegistry: MeterRegistry,
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
