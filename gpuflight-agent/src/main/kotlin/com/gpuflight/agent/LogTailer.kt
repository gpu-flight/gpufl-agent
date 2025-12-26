package com.gpuflight.com.gpuflight.agent

import com.gpuflight.com.gpuflight.agent.model.LogWrapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.io.RandomAccessFile
import kotlin.coroutines.coroutineContext

class LogTailer(
    private val folder: File,
    private val filePrefix: String,
    private val logType: String,
    private val cursorMgr: CursorManager,
    ) {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    private val streamKey = "$filePrefix.$logType"

    fun tail(): Flow<String> = flow {
        var (currentIndex, currentOffset) = cursorMgr.get(streamKey)

        println("[$logType] Starting at File Index $currentIndex, Offset: $currentOffset")

        while (currentCoroutineContext().isActive) {
            val file = getLogFile(currentIndex)

            if (!file.exists()) {
                println("[$logType] Waiting for file: ${file.absolutePath}...")
                delay(2000)

                if (checkForNewerFile(currentIndex)) {
                    println("[$logType] New file found, resetting cursor.")
                    currentIndex++
                    currentOffset = 0
                }
                continue
            }

            println("[$logType] Reading: ${file.name}")

            RandomAccessFile(file, "r").use { reader ->
                if (currentOffset > file.length()) currentOffset = 0
                reader.seek(currentOffset)

                var fileIsDone = false

                while (currentCoroutineContext().isActive && !fileIsDone) {
                    val fileLen = file.length()
                    println("[$logType] File length: $fileLen, current offset: $currentOffset")
                    if (fileLen > currentOffset) {
                        var line = reader.readLine()
                        while(line != null) {
                            if (line.isNotBlank()) {
                                val payload = processLine(line)
                                println("payload: $payload")
                                if (payload != null) emit(payload)
                            }
                            currentOffset = reader.filePointer
                            line = reader.readLine()
                        }
                        println("[$logType] Updated cursor for ${streamKey}: offset=$currentOffset")
                        cursorMgr.update(streamKey, currentIndex, currentOffset)
                    } else {
                        val nextFile = getLogFile(currentIndex + 1)
                        if (nextFile.exists()) {
                            println("[$logType] New file found, resetting cursor.")
                            fileIsDone = true
                            currentIndex++
                            currentOffset = 0
                        } else {
                            delay(100)
                        }
                    }

                }
            }

        }
    }.flowOn(Dispatchers.IO)

    private fun getLogFile(index: Int): File {
        // format: [prefix].log.[type].[index].log
        // e.g. gpufl.log.kernel.0.log
        return File(folder, "$filePrefix.$logType.$index.log")
    }

    private fun checkForNewerFile(index: Int): Boolean {
        return getLogFile(index + 1).exists()
    }

    private fun processLine(rawLine: String): String? {
        return try {
            val innerJson = json.parseToJsonElement(rawLine)
            println("innerJson: $innerJson")

            val wrapper = LogWrapper(
                src = logType,
                timestamp = System.currentTimeMillis(),
                data = innerJson
            )

            json.encodeToString(wrapper)
        } catch (e: Exception) {
            println("Failed to parse line: ${e.message}")
            null
        }
    }
}