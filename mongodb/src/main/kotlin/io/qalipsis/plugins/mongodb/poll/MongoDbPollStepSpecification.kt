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

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.mongodb.MongoDbRecord
import io.qalipsis.plugins.mongodb.MongoDbScenarioSpecification
import io.qalipsis.plugins.mongodb.MongoDbStepSpecification
import io.qalipsis.plugins.mongodb.Sorting
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
    StepSpecification<Unit, MongoDBPollResults, MongoDbPollStepSpecification>,
    MongoDbStepSpecification<Unit, MongoDBPollResults, MongoDbPollStepSpecification>,
    ConfigurableStepSpecification<Unit, MongoDBPollResults, MongoDbPollStepSpecification>,
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
     * Configures the monitoring of the poll step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)

    /**
     * Returns the documents individually.
     */
    fun flatten(): StepSpecification<Unit, MongoDbRecord, *>
}

/**
 * Implementation of [MongoDbPollStepSpecification].
 *
 * @author Alexander Sosnovsky
 */
@Spec
internal class MongoDbPollStepSpecificationImpl :
    AbstractStepSpecification<Unit, MongoDBPollResults, MongoDbPollStepSpecification>(),
    MongoDbPollStepSpecification {

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    internal var client: (() -> com.mongodb.reactivestreams.client.MongoClient) = { MongoClients.create() }

    internal var searchConfig = MongoDbSearchConfiguration()

    @field:NotNull
    internal var pollPeriod: Duration = Duration.ofSeconds(DefaultValues.pollDurationInSeconds)

    internal var flattenOutput = false

    internal var monitoringConfig = StepMonitoringConfiguration()

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

    override fun flatten(): StepSpecification<Unit, MongoDbRecord, *> {
        flattenOutput = true

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, MongoDbRecord, *>
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
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
data class MongoDbSearchConfiguration internal constructor(
    @field:NotBlank var database: String = "",
    @field:NotBlank var collection: String = "",
    @field:NotNull var query: Document = Document(),
    @field:NotEmpty var sort: LinkedHashMap<String, Sorting> = linkedMapOf(),
    @field:NotBlank var tieBreaker: String = "",
)

/**
 * Creates a Poll step in order to periodically fetch data from a MongoDb database.
 *
 * @author Alexander Sosnovsky
 */
fun MongoDbScenarioSpecification.poll(
    configurationBlock: MongoDbPollStepSpecification.() -> Unit
): MongoDbPollStepSpecification {
    val step = MongoDbPollStepSpecificationImpl()
    step.configurationBlock()

    (this as StepSpecificationRegistry).add(step)
    return step
}



