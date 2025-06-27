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

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.TestScenarioFactory
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.mongodb.Sorting
import io.qalipsis.plugins.mongodb.mongodb
import org.bson.Document
import org.junit.jupiter.api.Test
import java.time.Duration

internal class MongoDbSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario with default values`() {
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.mongodb().poll {
            name = "my-step"
            search {
                database = "db"
                collection = "col"
                query = Document()
                sort = linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                tieBreaker = "device"
            }
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(MongoDbPollStepSpecificationImpl::class).all {
            prop(MongoDbPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(MongoDbPollStepSpecificationImpl::client).isNotNull()
            prop(MongoDbPollStepSpecificationImpl::searchConfig).all {
                prop(MongoDbSearchConfiguration::database).isEqualTo("db")
                prop(MongoDbSearchConfiguration::collection).isEqualTo("col")
                prop(MongoDbSearchConfiguration::sort).isEqualTo(
                    linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                )
                prop(MongoDbSearchConfiguration::query).isEqualTo(Document())
                prop(MongoDbSearchConfiguration::tieBreaker).isEqualTo("device")
            }

            // check default values
            prop(MongoDbPollStepSpecificationImpl::client).isNotNull()
            prop(MongoDbPollStepSpecificationImpl::pollPeriod).isEqualTo(
                Duration.ofSeconds(DefaultValues.pollDurationInSeconds)
            )
            prop(MongoDbPollStepSpecificationImpl::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(MongoDbPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() {
        val clientBuilder: () -> com.mongodb.reactivestreams.client.MongoClient =
            { MongoClients.create("mongodb://localhost:27017/?streamType=netty") }
        val scenario = TestScenarioFactory.scenario("my-scenario") as StepSpecificationRegistry
        scenario.mongodb().poll {
            name = "my-step"
            connect(clientBuilder)
            search {
                database = "db"
                collection = "col"
                query = Document()
                sort = linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                tieBreaker = "device"
            }
            pollDelay(Duration.ofSeconds(1L))
            monitoring {
                events = true
                meters = true
            }
            broadcast(123, Duration.ofSeconds(20))
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(MongoDbPollStepSpecificationImpl::class).all {
            prop(MongoDbPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(MongoDbPollStepSpecificationImpl::client).isNotNull()
            prop(MongoDbPollStepSpecificationImpl::searchConfig).all {
                prop(MongoDbSearchConfiguration::database).isEqualTo("db")
                prop(MongoDbSearchConfiguration::collection).isEqualTo("col")
                prop(MongoDbSearchConfiguration::sort).isEqualTo(
                    linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                )
                prop(MongoDbSearchConfiguration::query).isEqualTo(Document())
                prop(MongoDbSearchConfiguration::tieBreaker).isEqualTo("device")
            }

            prop(MongoDbPollStepSpecificationImpl::pollPeriod).isEqualTo(Duration.ofSeconds(1L))
            prop(MongoDbPollStepSpecificationImpl::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            prop(MongoDbPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }

}
