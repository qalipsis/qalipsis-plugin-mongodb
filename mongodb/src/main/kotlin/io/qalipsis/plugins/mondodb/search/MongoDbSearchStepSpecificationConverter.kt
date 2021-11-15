package io.qalipsis.plugins.mondodb.search

import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbSearchStepSpecificationImpl
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentConverter
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentSearchBatchConverter
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentSearchSingleConverter
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
        val metrics = supplyIf(spec.metrics.meters) {
            buildMetrics(stepId)
        }

        @Suppress("UNCHECKED_CAST")
        val step = MongoDbSearchStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            mongoDbQueryClient = MongoDbQueryClientImpl(
                ioCoroutineScope,
                spec.clientFactory,
                metrics,
                eventsLogger.takeIf { spec.metrics.events }),
            databaseName = spec.searchConfig.database as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            collectionName = spec.searchConfig.collection as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            filter = spec.searchConfig.query as suspend (ctx: StepContext<*, *>, input: Any?) -> Document,
            sorting = spec.searchConfig.sort as suspend (ctx: StepContext<*, *>, input: Any?) -> LinkedHashMap<String, Sorting>,
            converter = buildConverter(spec) as MongoDbDocumentConverter<List<Document>, Any?, Any?>
        )
        creationContext.createdStep(step)
    }

    @KTestable
    private fun buildMetrics(stepId: StepName): MongoDbQueryMeterRegistry {
        return MongoDbQueryMeterRegistry(
            recordsCount = meterRegistry.counter("mongodb-search-received-records", "step", stepId),
            failureCounter = meterRegistry.counter("mongodb-search-failures", "step", stepId),
            successCounter = meterRegistry.counter("mongodb-search-successes", "step", stepId),
            timeToResponse = meterRegistry.timer("mongodb-search-time-to-response", "step", stepId)
        )
    }

    private fun buildConverter(
        spec: MongoDbSearchStepSpecificationImpl<*>
    ): MongoDbDocumentConverter<MongoDBQueryResult, *, *> {

        return if (spec.flattenOutput) {
            MongoDbDocumentSearchSingleConverter<Any>()
        } else {
            MongoDbDocumentSearchBatchConverter<Any>()
        }
    }

}
