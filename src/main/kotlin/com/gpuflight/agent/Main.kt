package com.gpuflight.com.gpuflight.agent

import com.gpuflight.com.gpuflight.agent.model.AgentConfig
import com.gpuflight.com.gpuflight.agent.publisher.PublisherFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.system.exitProcess

fun main(args: Array<String>) = runBlocking {
    val configPath = parseConfigArg(args)
    val config = if (configPath != null) {
        loadExternalConfig(configPath)
    } else {
        val env = parseEnvArg(args) ?: "local"
        loadConfig(env)
    }
    val publisher = PublisherFactory.create(config.publisher)

    val logTypes = listOf("kernel", "scope", "system")
    val source = config.source
    val folder = File(source.folder)
    val cursorMgr = CursorManager(File("./cursor.json"))

    val jobs = logTypes.map { type ->
        launch(Dispatchers.IO) {
            val tailer = LogTailer(folder, source.filePrefix, type, cursorMgr)

            tailer.tail().collect { logWrapper ->
                publisher.publish("gpu-trace-events", type, logWrapper)
            }
        }
    }

    publisher.use { publisher ->
        joinAll(*jobs.toTypedArray())
    }
}

fun parseEnvArg(args: Array<String>): String? {
    for (arg in args) {
        if (arg.startsWith("--env=")) {
            return arg.substring("--env=".length)
        }
    }

    return args.firstOrNull { !it.startsWith("--") }
}

fun parseConfigArg(args: Array<String>): String? {
    for (arg in args) {
        if (arg.startsWith("--config=")) {
            return arg.substring("--config=".length)
        }
    }
    return null
}

fun loadExternalConfig(path: String): AgentConfig {
    val file = File(path)
    if (!file.exists()) {
        System.err.println("❌ Config file not found: '$path'")
        exitProcess(1)
    }

    try {
        val jsonText = file.readText()
        val json = Json {
            ignoreUnknownKeys = true
            classDiscriminator = "type"
        }
        return json.decodeFromString<AgentConfig>(jsonText)
    } catch (e: Exception) {
        System.err.println("Failed to parse config file: ${e.message}")
        exitProcess(1)
    }
}

fun loadConfig(env: String): AgentConfig {
    val path = "config/$env.json"

    val stream = Thread.currentThread().contextClassLoader.getResourceAsStream(path)

    if (stream == null) {
        System.err.println("❌ Config resource not found: '$path'")
        System.err.println("   Ensure files are in 'src/main/resources/config/'")
        exitProcess(1)
    }

    try {
        val jsonText = stream.bufferedReader().use { it.readText() }

        val json = Json {
            ignoreUnknownKeys = true
            classDiscriminator = "type"
        }

        return json.decodeFromString<AgentConfig>(jsonText)
    } catch (e: Exception) {
        System.err.println("Failed to parse config file: ${e.message}")
        exitProcess(1)
    }

}