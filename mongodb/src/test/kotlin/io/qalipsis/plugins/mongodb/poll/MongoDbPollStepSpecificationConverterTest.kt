package io.qalipsis.plugins.mongodb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.invokeInvisible
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.mondodb.MongoDBQueryResult
import io.qalipsis.plugins.mondodb.MongoDbPollStepSpecificationImpl
import io.qalipsis.plugins.mondodb.Sorting
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentPollBatchConverter
import io.qalipsis.plugins.mondodb.converters.MongoDbDocumentPollSingleConverter
import io.qalipsis.plugins.mondodb.poll.MongoDbIterativeReader
import io.qalipsis.plugins.mondodb.poll.MongoDbPollStepSpecificationConverter
import io.qalipsis.plugins.mondodb.search.MongoDbQueryMeterRegistry
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import org.bson.Document
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

/**
 *
 * @author Maxim Golokhov
 */
@WithMockk
internal class MongoDbPollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<MongoDbPollStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var mockedClientBuilder: () -> com.mongodb.reactivestreams.client.MongoClient

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<MongoDbPollStepSpecificationImpl>()))
            .isTrue()
    }

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(5)
    fun `should convert with name and metrics`() = runBlockingTest {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect(mockedClientBuilder)
            search {
                database = "db"
                collection = "col"
                query = Document()
                sort = linkedMapOf("device" to Sorting.ASC, "event" to Sorting.ASC)
                tieBreaker = "device"
            }
            metrics {
                meters = true
            }
            pollDelay(10_000L)
            broadcast(123, Duration.ofSeconds(20))
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)
        val meterRegistry = relaxedMockk<MongoDbQueryMeterRegistry>()

        val recordsConverter: DatasourceObjectConverter<MongoDBQueryResult, out Any> = relaxedMockk()
        every { spiedConverter["buildConverter"](refEq(spec)) } returns recordsConverter
        every { spiedConverter["buildMetrics"](eq("my-step")) } returns meterRegistry

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<MongoDbPollStepSpecificationImpl>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("id").isEqualTo("my-step")
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(recordsConverter)
                prop("reader").isNotNull().isInstanceOf(MongoDbIterativeReader::class).all {
                    prop("clientBuilder").isSameAs(mockedClientBuilder)
                    prop("eventsLogger").isNull()
                    prop("mongoDbPollMeterRegistry").isSameAs(meterRegistry)
                }
            }
        }
        verifyOnce { spiedConverter["buildConverter"](refEq(spec)) }

        val channelFactory = creationContext.createdStep!!
            .getProperty<MongoDbIterativeReader>("reader")
            .getProperty<() -> Channel<List<Document>>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    internal fun `should build batch converter`() {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.search {
            database = "db"
            collection = "coll"
        }
        spec.flattenOutput = false

        // when
        val converter = converter.invokeInvisible<DatasourceObjectConverter<MongoDBQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(MongoDbDocumentPollBatchConverter::class).all {
            prop("databaseName").isEqualTo("db")
            prop("collectionName").isEqualTo("coll")
        }
    }

    @Test
    internal fun `should build single converter`() {
        // given
        val spec = MongoDbPollStepSpecificationImpl()
        spec.search {
            database = "db"
            collection = "coll"
        }
        spec.flattenOutput = true

        // when
        val converter = converter.invokeInvisible<DatasourceObjectConverter<MongoDBQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(MongoDbDocumentPollSingleConverter::class).all {
            prop("databaseName").isEqualTo("db")
            prop("collectionName").isEqualTo("coll")
        }
    }

    // TODO Test the builder method for the meter registry.
}
