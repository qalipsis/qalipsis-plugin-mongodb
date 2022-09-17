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

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
    `java-test-fixtures`
}

description = "QALIPSIS plugin for MongoDB"

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_11.majorVersion
        javaParameters = true
    }
}

allOpen {
    annotations(
        "io.micronaut.aop.Around",
        "jakarta.inject.Singleton",
        "io.qalipsis.api.annotations.StepConverter",
        "io.qalipsis.api.annotations.StepDecorator",
        "io.qalipsis.api.annotations.PluginComponent",
        "io.qalipsis.api.annotations.Spec",
        "io.micronaut.validation.Validated"
    )
}

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

val coreVersion: String by project
val mongoDbVersion = "4.7.1"

dependencies {
    implementation(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    compileOnly("io.aeris-consulting:catadioptre-annotations")
    compileOnly("io.micronaut:micronaut-runtime")
    api("org.mongodb:mongodb-driver-reactivestreams:${mongoDbVersion}")
    api("org.mongodb:mongodb-driver-sync:${mongoDbVersion}")

    api("io.qalipsis:api-common")
    api("io.qalipsis:api-dsl")

    kapt(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kapt("io.qalipsis:api-processors")
    kapt("io.qalipsis:api-dsl")
    kapt("io.qalipsis:api-common")
    kapt("io.aeris-consulting:catadioptre-annotations")

    testImplementation("org.testcontainers:mongodb")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    testImplementation("io.qalipsis:test")
    testImplementation("io.qalipsis:api-dsl")
    testImplementation(testFixtures("io.qalipsis:api-dsl"))
    testImplementation(testFixtures("io.qalipsis:api-common"))
    testImplementation(testFixtures("io.qalipsis:runtime"))
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testImplementation("io.aeris-consulting:catadioptre-kotlin")
    testRuntimeOnly("io.qalipsis:runtime")
    testRuntimeOnly("io.qalipsis:head")
    testRuntimeOnly("io.qalipsis:factory")

    kaptTest(platform("io.qalipsis:plugin-platform:${coreVersion}"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors")
}

