package io.qalipsis.plugins.mondodb.poll

import org.bson.Document
import org.bson.conversions.Bson

/**
 * statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Alexander Sosnovsky
 */
internal interface PollStatement {

    /**
     * a constructed find clause as BSON
     */
    val filter: Bson

    /**
     * a constructed sort clause as BSON
     */
    val sorting: Bson

    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(document: Document)

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()

}