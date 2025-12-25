plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.0.0"
}

group = "com.gpuflight"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.slf4j:slf4j-simple:2.0.12")
}

kotlin {
    jvmToolchain(24)
}

tasks.test {
    useJUnitPlatform()
}