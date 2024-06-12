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
