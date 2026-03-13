package com.gpuflight.com.gpuflight.agent

import com.gpuflight.com.gpuflight.agent.model.LogWrapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.io.RandomAccessFile

/**
 * Tails log files written by the C++ gpufl client.
 *
 * File naming convention produced by the C++ LogFileRotator:
 *   Active file  : <prefix>.<channel>.log         (no numeric suffix; always being written)
 *   Rotated files: <prefix>.<channel>.1.log        (most recently rotated)
 *                  <prefix>.<channel>.2.log        (older), …
 *
 * Cursor fileIndex semantics:
 *   0   → currently tailing the active file
 *   N≥1 → finishing the just-rotated file .<N>.log after a rotation event,
 *          then returning to index 0 for the new active file
 */
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

    /** The active file written by the C++ client: <prefix>.<type>.log */
    private fun activeFile(): File = File(folder, "$filePrefix.$logType.log")

    /** A completed, rotated file: <prefix>.<type>.<index>.log  (index ≥ 1) */
    private fun rotatedFile(index: Int): File = File(folder, "$filePrefix.$logType.$index.log")

    fun tail(): Flow<LogWrapper> = flow {
        var (currentIndex, currentOffset) = cursorMgr.get(streamKey)

        println("[$logType] Starting – fileIndex=$currentIndex, offset=$currentOffset")

        while (currentCoroutineContext().isActive) {
            val file = if (currentIndex == 0) activeFile() else rotatedFile(currentIndex)

            if (!file.exists()) {
                if (currentIndex > 0) {
                    // The rotated file has already been removed (e.g. agent restarted long
                    // after the rotation completed and the file was aged out).
                    // Fall back to the active file to avoid getting stuck.
                    println("[$logType] Rotated file ${file.name} gone, resuming active file.")
                    currentIndex = 0
                    currentOffset = 0
                    cursorMgr.update(streamKey, currentIndex, currentOffset)
                } else {
                    println("[$logType] Waiting for active file: ${file.absolutePath}…")
                    delay(2000)
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

                    if (fileLen > currentOffset) {
                        var line = reader.readLine()
                        while (line != null) {
                            if (line.isNotBlank()) {
                                processLine(line)?.let { emit(it) }
                            }
                            currentOffset = reader.filePointer
                            line = reader.readLine()
                        }
                        cursorMgr.update(streamKey, currentIndex, currentOffset)
                    } else {
                        // No new bytes in this file.
                        if (currentIndex == 0) {
                            // Tailing the active file: check whether rotation just happened.
                            // On rotation the C++ client renames the active file to .1.log and
                            // creates a new empty active file.  .1.log must be at least as large
                            // as our current offset (it *is* the old active, just renamed).
                            val rotated = rotatedFile(1)
                            if (rotated.exists() && rotated.length() >= currentOffset) {
                                println("[$logType] Rotation detected – switching to ${rotated.name} at offset $currentOffset")
                                currentIndex = 1
                                // Intentionally keep currentOffset so we skip bytes already sent.
                                cursorMgr.update(streamKey, currentIndex, currentOffset)
                                fileIsDone = true
                            } else {
                                delay(100)
                            }
                        } else {
                            // Finished draining the rotated file.
                            // Return to the new active file from the beginning.
                            println("[$logType] Finished ${file.name}, resuming active file.")
                            currentIndex = 0
                            currentOffset = 0
                            cursorMgr.update(streamKey, currentIndex, currentOffset)
                            fileIsDone = true
                        }
                    }
                }
            }
        }
    }.flowOn(Dispatchers.IO)

    private fun processLine(rawLine: String): LogWrapper? {
        return try {
            val innerJson = json.parseToJsonElement(rawLine)
            val type = innerJson.jsonObject["type"]?.jsonPrimitive?.content ?: "unknown"
            val inet = java.net.InetAddress.getLocalHost()

            LogWrapper(
                agentSendingTime = System.currentTimeMillis(),
                data = rawLine,
                type = type,
                hostname = inet.hostName ?: "unknown",
                ipAddr = inet.hostAddress ?: "unknown",
            )
        } catch (e: Exception) {
            println("Failed to parse line: ${e.message}")
            null
        }
    }
}
