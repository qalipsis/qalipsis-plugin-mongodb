package io.qalipsis.plugins.mondodb

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.mondodb.poll.MongoDBPollResults
import org.bson.Document
import java.time.Duration
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

/**
 * Specification for an [io.qalipsis.api.steps.datasource.IterativeDatasourceStep] to poll data from MongoDb.
 *
 * @author Alexander Sosnovsky
 */
interface MongoDbPollStepSpecification :
    StepSpecification<Unit, MongoDBPollResults, Flattenable<MongoDbRecord, MongoDBPollResults>>,
    MongoDbStepSpecification<Unit, MongoDBPollResults, Flattenable<MongoDbRecord, MongoDBPollResults>>,
    Flattenable<MongoDbRecord, MongoDBPollResults>,
    LoopableSpecification, UnicastSpecification, BroadcastSpecification {

    /**
     * Configures connection to the MongoDb.
     */
    fun connect(client: () -> com.mongodb.reactivestreams.client.MongoClient)

    /**
     * Defines the prepared statement to execute when polling. The query must contain ordering clauses, the tie-breaker
     * column being set as first column to sort.
     */
    fun search(searchConfiguration: MongoDbSearchConfiguration.() -> Unit)

    /**
     * Duration between two executions of poll. Default value is 10 seconds.
     */
    fun pollDelay(duration: Duration)

    /**
     * Duration between two executions of poll. Default value is 10 seconds.
     */
    fun pollDelay(delayMillis: Long)

    /**
     * Configures the metrics of the poll step.
     */
    fun metrics(metricsConfiguration: MongoDbMetricsConfiguration.() -> Unit)
}

/**
 * Implementation of [MongoDbPollStepSpecification].
 *
 * @author Alexander Sosnovsky
 */
@Spec
class MongoDbPollStepSpecificationImpl :
    AbstractStepSpecification<Unit, MongoDBPollResults, Flattenable<MongoDbRecord, MongoDBPollResults>>(),
    Flattenable<MongoDbRecord, MongoDBPollResults>,
    MongoDbPollStepSpecification {

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    internal var client: (() -> com.mongodb.reactivestreams.client.MongoClient) = { MongoClients.create() }

    internal var searchConfig = MongoDbSearchConfiguration()

    @field:NotNull
    internal var pollPeriod: Duration = Duration.ofSeconds(DefaultValues.pollDurationInSeconds)

    internal val metrics = MongoDbMetricsConfiguration()

    internal var flattenOutput = false

    override fun connect(client: () -> com.mongodb.reactivestreams.client.MongoClient) {
        this.client = client
    }

    override fun search(searchConfiguration: MongoDbSearchConfiguration.() -> Unit) {
        searchConfig.searchConfiguration()
    }

    override fun pollDelay(delayMillis: Long) {
        pollPeriod = Duration.ofMillis(delayMillis)
    }

    override fun pollDelay(duration: Duration) {
        pollPeriod = duration
    }

    override fun metrics(metricsConfiguration: MongoDbMetricsConfiguration.() -> Unit) {
        metrics.metricsConfiguration()
    }

    override fun flatten(): StepSpecification<Unit, MongoDbRecord, *> {
        flattenOutput = true

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, MongoDbRecord, *>
    }

}

internal object DefaultValues {
    const val pollDurationInSeconds = 10L
}

/**
 * @property database name of db to search
 * @property collection collection in db (table in sql)
 * @property query [Document] query for search
 * @property sort ordering clause
 * @property tieBreaker defines the name, which is the value used to limit the records for the next poll.
 * The tie-breaker must be used as the first sort clause of the query and always be not null. Only the records
 * from the database having a [tieBreaker] greater (or less if sorted descending) than the last polled value will be fetched at next poll.
 */
@Spec
data class MongoDbSearchConfiguration(
    @field:NotBlank var database: String = "",
    @field:NotBlank var collection: String = "",
    @field:NotNull var query: Document = Document(),
    @field:NotEmpty var sort: LinkedHashMap<String, Sorting> = linkedMapOf(),
    @field:NotBlank var tieBreaker: String = "",
)

/**
 * Configuration of the metrics to record for the MongoDb [poll] step.
 *
 * @property events when true, records the events of the step, defaults to false.
 * @property meters when true, records the meters of the step, defaults to false.
 *
 * @author Maxim Golokhov
 */
@Spec
data class MongoDbMetricsConfiguration(
    var events: Boolean = false,
    var meters: Boolean = false
)

/**
 * Creates a Poll step in order to periodically fetch data from a MongoDb database.
 *
 * @author Alexander Sosnovsky
 */
fun MongoDbScenarioSpecification.poll(
    configurationBlock: MongoDbPollStepSpecificationImpl.() -> Unit
): MongoDbPollStepSpecification {
    val step = MongoDbPollStepSpecificationImpl()
    step.configurationBlock()

    (this as StepSpecificationRegistry).add(step)
    return step
}



