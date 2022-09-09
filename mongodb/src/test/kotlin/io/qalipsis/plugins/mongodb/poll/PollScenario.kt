package io.qalipsis.plugins.mongodb.poll

import com.mongodb.reactivestreams.client.MongoClients
import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.lang.concurrentList
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.rampup.regular
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.filterNotNull
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.logErrors
import io.qalipsis.api.steps.map
import io.qalipsis.api.steps.onEach
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.mongodb
import io.qalipsis.plugins.mondodb.poll.poll
import org.bson.BsonTimestamp
import org.bson.Document
import java.time.Duration

/**
 *
 * Scenario to demo how the poll step can work. The scenario reads the entries in a building on one side and the exits
 * on the other side.
 *
 * Records related to the same person are joined and the duration is then printed out in the console.
 *
 * @author Eric Jessé
 */
object PollScenario {

    private const val minions = 5

    val receivedMessages = concurrentList<String>()

    var mongoDbPort: Int = 0

    @JvmStatic
    private val log = logger()

    @Scenario
    fun pollData() {
        scenario("mongodb-poll") {
            minionsCount = minions
            rampUp {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }
            .start()
            .mongodb().poll {
                name = "poll.in"
                connect { MongoClients.create("mongodb://localhost:${mongoDbPort}/?streamType=netty") }
                search {
                    database = "the-db"
                    collection = "moves"
                    query = Document().append("action", "IN")
                    sort = linkedMapOf("timestamp" to Sorting.ASC)
                    tieBreaker = "timestamp"
                }
                pollDelay(Duration.ofSeconds(1))
            }.flatten()
            .map { it.value }
            .logErrors()
            .innerJoin(
                using = { it.value["username"] },
                on = {
                    it.mongodb().poll {
                        name = "poll.out"
                        connect { MongoClients.create("mongodb://localhost:${mongoDbPort}/?streamType=netty") }
                        search {
                            database = "the-db"
                            collection = "moves"
                            query = Document().append("action", "OUT")
                            sort = linkedMapOf("timestamp" to Sorting.ASC)
                            tieBreaker = "timestamp"
                        }
                        pollDelay(Duration.ofSeconds(1))
                    }
                        .flatten()
                        .logErrors()
                        .map {
                            log.trace { "Right record: $it" }
                            it.value
                        }
                },
                having = { it.value["username"].also { log.trace { "Right: $it" } } }
            )
            .filterNotNull()
            .map { (inAction, outAction) ->
                val epochSecondIn = (inAction["timestamp"] as BsonTimestamp).value
                val epochSecondOut = (outAction["timestamp"] as BsonTimestamp).value

                inAction["username"] to Duration.ofSeconds(epochSecondOut - epochSecondIn)
            }
            .map { "The user ${it.first} stayed ${it.second.toMinutes()} minute(s) in the building" }
            .onEach {
                receivedMessages.add(it)
            }
            .onEach {
                println(it)
            }
    }

}
