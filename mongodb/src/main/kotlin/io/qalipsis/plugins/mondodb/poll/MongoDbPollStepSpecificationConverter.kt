package io.qalipsis.plugins.mondodb.poll

import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepName
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbPollStepSpecificationImpl
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentPollBatchConverter
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentPollSingleConverter
import io.qalipsis.plugins.mondodb.search.MongoDbQueryMeterRegistry
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
    private var eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val coroutineScope: CoroutineScope
) : StepSpecificationConverter<MongoDbPollStepSpecificationImpl> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is MongoDbPollStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<MongoDbPollStepSpecificationImpl>) {
        val spec = creationContext.stepSpecification
        val pollStatement = buildPollStatement(spec)
        val stepId = spec.name

        val mongoDbPollMetrics = supplyIf(spec.metrics.meters) {
            buildMetrics(spec.name)
        }

        val reader = MongoDbIterativeReader(
            coroutineScope = coroutineScope,
            clientBuilder = spec.client,
            pollStatement = pollStatement,
            pollDelay = spec.pollPeriod,
            eventsLogger = eventsLogger.takeIf { spec.metrics.events },
            mongoDbPollMeterRegistry = mongoDbPollMetrics
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

    @KTestable
    private fun buildMetrics(stepId: StepName): MongoDbQueryMeterRegistry {
        return MongoDbQueryMeterRegistry(
            recordsCount = meterRegistry.counter("mongodb-poll-received-records", "step", stepId),
            failureCounter = meterRegistry.counter("mongodb-poll-failures", "step", stepId),
            successCounter = meterRegistry.counter("mongodb-poll-successes", "step", stepId),
            timeToResponse = meterRegistry.timer("mongodb-poll-time-to-response", "step", stepId)
        )
    }
}
