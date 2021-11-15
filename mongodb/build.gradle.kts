import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
    `java-test-fixtures`
}

description = "Qalipsis Plugins - MongoDB"

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

val micronautVersion: String by project
val kotlinCoroutinesVersion: String by project
val testContainersVersion: String by project
val nettyVersion = "4.1.51.Final"
val catadioptreVersion: String by project

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

dependencies {
    compileOnly("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")
    compileOnly(kotlin("stdlib"))
    compileOnly(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    compileOnly("io.micronaut:micronaut-runtime")
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:${kotlinCoroutinesVersion}")
    api("org.mongodb:mongodb-driver-reactivestreams:4.1.1")
    api("org.mongodb:mongodb-driver-sync:4.1.1")
    implementation(platform("io.netty:netty-bom:$nettyVersion"))

    api("io.qalipsis:api-common:${project.version}")
    api("io.qalipsis:api-dsl:${project.version}")

    kapt(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    kapt("io.qalipsis:api-processors:${project.version}")
    kapt("io.qalipsis:api-dsl:${project.version}")
    kapt("io.qalipsis:api-common:${project.version}")
    kapt("io.aeris-consulting:catadioptre-annotations:${catadioptreVersion}")

    testImplementation("org.testcontainers:mongodb:${testContainersVersion}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${kotlinCoroutinesVersion}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.4.1")
    testImplementation("io.qalipsis:test:${project.version}")
    testImplementation("io.qalipsis:api-dsl:${project.version}")
    testImplementation(testFixtures("io.qalipsis:api-dsl:${project.version}"))
    testImplementation(testFixtures("io.qalipsis:api-common:${project.version}"))
    testImplementation(testFixtures("io.qalipsis:runtime:${project.version}"))
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testImplementation("io.aeris-consulting:catadioptre-kotlin:${catadioptreVersion}")
    testRuntimeOnly("io.qalipsis:runtime:${project.version}")

    kaptTest(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:api-processors:${project.version}")
}

