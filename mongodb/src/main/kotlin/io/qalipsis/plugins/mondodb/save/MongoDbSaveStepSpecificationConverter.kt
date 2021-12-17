package io.qalipsis.plugins.mondodb.save

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
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

        @Suppress("UNCHECKED_CAST")
        val step = MongoDbSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            mongoDbSaveQueryClient = MongoDbSaveQueryClientImpl(
                ioCoroutineScope,
                spec.clientBuilder,
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),
            databaseName = spec.queryConfig.database as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            collectionName = spec.queryConfig.collection as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            recordsFactory = spec.queryConfig.records as suspend (ctx: StepContext<*, *>, input: I) -> List<Document>,
        )
        creationContext.createdStep(step)
    }

}
