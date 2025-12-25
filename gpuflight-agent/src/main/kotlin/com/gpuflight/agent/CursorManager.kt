package com.gpuflight.com.gpuflight.agent

import kotlinx.serialization.json.Json
import java.io.File

class CursorManager(private val cursorFile: File) {
    private val json = Json { ignoreUnknownKeys = true; prettyPrint = true }
    val state: CursorState = load()

    private fun load(): CursorState {
        if (!cursorFile.exists()) return CursorState()
        return try {
            json.decodeFromString<CursorState>(cursorFile.readText())
        } catch (e: Exception) {
            println("Corrupt cursor file. Starting fresh.")
            CursorState()
        }
    }

    @Synchronized
    fun save() {
        try {
            val tempFile = File(cursorFile.absolutePath + ".tmp")
            tempFile.writeText(json.encodeToString(CursorState.serializer(), state))
            if (cursorFile.exists()) cursorFile.delete()
            tempFile.renameTo(cursorFile)
        } catch (e: Exception) {
            println("Failed to save cursor: ${e.message}")
        }
    }

    fun update(streamKey: String, fileIndex: Int, offset: Long) {
        state.streams[streamKey] = CursorPosition(fileIndex, offset)
    }

    fun get(streamKey: String): CursorPosition {
        return state.streams[streamKey] ?: CursorPosition(0, 0L)
    }
}