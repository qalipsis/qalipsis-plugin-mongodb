package io.qalipsis.plugins.mondodb.converters;

import io.qalipsis.api.context.StepOutput
import java.util.concurrent.atomic.AtomicLong

interface MongoDbDocumentConverter<R, O, I> {
    /**
     * Sends [value] to the [output] channel in any form.
     *
     * @param offset an reference to the offset, it is up to the implementation to increment it
     * @param value input value to send after any conversion to the output
     * @param input received in the channel to be send along with the data after conversion
     * @param output channel to received the data after conversion
     */
    suspend fun supply(
        offset: AtomicLong,
        value: R,
        databaseName: String,
        collectionName: String,
        input: I,
        output: StepOutput<O>
    )
}
