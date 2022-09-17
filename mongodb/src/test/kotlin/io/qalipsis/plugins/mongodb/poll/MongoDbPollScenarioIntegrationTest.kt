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
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import com.mongodb.client.result.InsertManyResult
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.plugins.mongodb.Constants
import io.qalipsis.plugins.mongodb.configuration.AbstractMongoDbIntegrationTest
import io.qalipsis.runtime.test.QalipsisTestRunner
import io.qalipsis.test.io.readResourceLines
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import kotlin.math.pow

/**
 * Integration test to demo the usage of the poll operator in a scenario.
 *
 * See [PollScenario] for more details.
 *
 * @author Eric Jessé
 */
@Testcontainers
internal class MongoDbPollScenarioIntegrationTest() {

    @Test
    @Timeout(30)
    internal fun `should run the poll scenario`() {
        // given
        PollScenario.mongoDbPort = mongodb.getMappedPort(27017)
        val client = MongoClients.create("mongodb://localhost:${PollScenario.mongoDbPort}/?streamType=netty")
        populateMongoFromCsv(client, "input/building-moves.csv")

        // when
        val exitCode = QalipsisTestRunner.withScenarios("mongodb-poll").execute()

        // then
        Assertions.assertEquals(0, exitCode)
        assertThat(PollScenario.receivedMessages).all {
            hasSize(5)
            containsExactlyInAnyOrder(
                "The user alice stayed 48 minute(s) in the building",
                "The user bob stayed 20 minute(s) in the building",
                "The user charles stayed 1 minute(s) in the building",
                "The user david stayed 114 minute(s) in the building",
                "The user erin stayed 70 minute(s) in the building"
            )
        }
    }

    fun populateMongoFromCsv(client: MongoClient, name: String) {
        val documents = documentsFromCsv(name)
        val countDownLatch = CountDownLatch(1)
        client.getDatabase("the-db").getCollection("moves")
            .insertMany(documents).subscribe(object : Subscriber<InsertManyResult> {
                override fun onSubscribe(s: Subscription) {
                    s.request(Long.MAX_VALUE)
                }

                override fun onNext(document: InsertManyResult) {
                    AbstractMongoDbIntegrationTest.log.debug { "Inserted: $document" };
                }

                override fun onError(error: Throwable) {
                    AbstractMongoDbIntegrationTest.log.debug { "Failed" };
                }

                override fun onComplete() {
                    AbstractMongoDbIntegrationTest.log.debug { "DB is populated" }
                    countDownLatch.countDown()
                }
            })
        countDownLatch.await()
    }

    fun documentsFromCsv(name: String): List<Document> {
        return this.readResourceLines(name)
            .map {
                val values = it.split(",")
                val timestamp = Instant.parse(values[0]).epochSecond
                Document("timestamp", BsonTimestamp(timestamp))
                    .append("action", values[1])
                    .append("username", values[2])
            }
    }

    companion object {

        @Container
        @JvmStatic
        val mongodb = MongoDBContainer(DockerImageName.parse(Constants.DOCKER_IMAGE))
            .apply {
                waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
            }

    }
}
