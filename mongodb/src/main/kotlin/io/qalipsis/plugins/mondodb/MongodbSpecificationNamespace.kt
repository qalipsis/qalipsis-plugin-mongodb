package io.qalipsis.plugins.mondodb

import io.qalipsis.api.scenario.ScenarioSpecification
import io.qalipsis.api.steps.AbstractPluginStepWrapper
import io.qalipsis.api.steps.AbstractScenarioSpecificationWrapper
import io.qalipsis.api.steps.StepSpecification


/**
 * Step wrapper to append to all steps before using a step from the MongoDB plugin.
 *
 * @author Maxim Golokhov
 */
interface MongoDbStepSpecification<INPUT, OUTPUT, SELF : StepSpecification<INPUT, OUTPUT, SELF>> :
    StepSpecification<INPUT, OUTPUT, SELF>

/**
 * Step wrapper to append to all steps before using a step from the MongoDB plugin.
 *
 * @author Maxim Golokhov
 */
class MongoDbSpecificationImpl<INPUT, OUTPUT>(wrappedStepSpec: StepSpecification<INPUT, OUTPUT, *>) :
    AbstractPluginStepWrapper<INPUT, OUTPUT>(wrappedStepSpec),
    MongoDbStepSpecification<INPUT, OUTPUT, AbstractPluginStepWrapper<INPUT, OUTPUT>>

fun <INPUT, OUTPUT> StepSpecification<INPUT, OUTPUT, *>.mongodb(): MongoDbStepSpecification<INPUT, OUTPUT, *> =
    MongoDbSpecificationImpl(this)

/**
 * Scenario wrapper to append to a scenario before using a step from the MongoDB plugin.
 *
 * @author Maxim Golokhov
 */
class MongoDbScenarioSpecification(scenario: ScenarioSpecification) :
    AbstractScenarioSpecificationWrapper(scenario)

fun ScenarioSpecification.mongodb() = MongoDbScenarioSpecification(this)
