package io.qalipsis.plugins.mondodb.save

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
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import org.bson.Document

/**
 * [StepSpecificationConverter] from [MongoDbSaveStepSpecificationImpl] to [MongoDbSaveStep]
 * to use the Save API.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class MongoDbSaveStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<MongoDbSaveStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is MongoDbSaveStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<MongoDbSaveStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name
        val metrics = supplyIf(spec.metrics.meters) {
            buildMetrics(stepId)
        }

        @Suppress("UNCHECKED_CAST")
        val step = MongoDbSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            mongoDbSaveQueryClient = MongoDbSaveQueryClientImpl(
                ioCoroutineScope,
                spec.clientBuilder,
                metrics,
                eventsLogger.takeIf { spec.metrics.events }),
            databaseName = spec.queryConfig.database as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            collectionName = spec.queryConfig.collection as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            recordsFactory = spec.queryConfig.records as suspend (ctx: StepContext<*, *>, input: I) -> List<Document>,
        )
        creationContext.createdStep(step)
    }

    @KTestable
    private fun buildMetrics(stepId: StepName): MongoDbSaveQueryMeterRegistry {
        return MongoDbSaveQueryMeterRegistry(
            recordsCount = meterRegistry.counter("mongodb-save-saved-records", "step", stepId),
            failureCounter = meterRegistry.counter("mongodb-save-failures", "step", stepId),
            successCounter = meterRegistry.counter("mongodb-save-successes", "step", stepId),
            timeToResponse = meterRegistry.timer("mongodb-save-time-to-response", "step", stepId)
        )
    }

}
