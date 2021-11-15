package io.qalipsis.plugins.mondodb

import io.qalipsis.api.steps.StepSpecification

/**
 * Interface of a step that provides a list of items by default but can be amended to flatten those lists.
 *
 * @author Alexander Sosnovsky
 */
interface Flattenable<T, I : Iterable<T>> : StepSpecification<Unit, I, Flattenable<T, I>> {

    /**
     * Returns each record of a batch individually to the next steps.
     */
    fun flatten(): StepSpecification<Unit, T, *>

}
