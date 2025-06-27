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

package io.qalipsis.plugins.mongodb.configuration

import com.mongodb.client.result.InsertManyResult
import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import com.mongodb.reactivestreams.client.MongoCollection
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.mongodb.Constants
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.io.readResourceLines
import io.qalipsis.test.mockk.WithMockk
import org.bson.BsonTimestamp
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Timeout
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch
import kotlin.math.pow

@WithMockk
@Testcontainers
internal abstract class AbstractMongoDbIntegrationTest {

    lateinit var client: MongoClient

    lateinit var collection: MongoCollection<Document>

    val testDispatcherProvider = TestDispatcherProvider()

    private fun stringTimeToEpochMilli(string: String): Long {
        val formatter = DateTimeFormatter.ofPattern("yyyy-M-d'T'HH:mm:ss")
        val time = LocalDateTime.parse(string, formatter)
        return time.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli()
    }

    @BeforeAll
    fun beforeAll() {
        client = MongoClients.create("mongodb://localhost:${mongodb.getMappedPort(27017)}/?streamType=netty")
        val database = client.getDatabase("db")
        collection = database.getCollection("col")
    }

    @AfterAll
    fun afterAll() {
        client.close()
    }

    @BeforeEach
    @Timeout(5)
    fun setUp() {
        val countDownLatch = CountDownLatch(1)
        collection.drop().subscribe(object : Subscriber<Void> {
            override fun onSubscribe(s: Subscription) {
                s.request(Long.MAX_VALUE)
            }

            override fun onNext(document: Void) {
                log.debug { "next" };
            }

            override fun onError(error: Throwable) {
                log.debug { "Failed" };
            }

            override fun onComplete() {
                log.debug { "completed" }
                countDownLatch.countDown()
            }
        })
        countDownLatch.await()
    }

    fun documentsFromCsv(name: String): List<Document> {
        return this.readResourceLines(name)
            .map {
                val values = it.split(";")
                val timestamp = stringTimeToEpochMilli(values[0])
                Document("time", BsonTimestamp(timestamp))
                    .append("device", values[1])
                    .append("event", values[2])
            }
    }

    fun populateMongoFromCsv(name: String) {
        log.debug { "populate DB from CVS" }
        val documents = documentsFromCsv(name)
        val countDownLatch = CountDownLatch(1)
        collection.insertMany(documents).subscribe(object : Subscriber<InsertManyResult> {
            override fun onSubscribe(s: Subscription) {
                s.request(Long.MAX_VALUE)
            }

            override fun onNext(document: InsertManyResult) {
                log.debug { "Inserted: $document" };
            }

            override fun onError(error: Throwable) {
                log.debug { "Failed" };
            }

            override fun onComplete() {
                log.debug { "DB is populated" }
                countDownLatch.countDown()
            }
        })
        countDownLatch.await()
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

        @JvmStatic
        val log = logger()
    }

}
