package com.gpuflight.com.gpuflight.agent

import kotlinx.serialization.Serializable
import java.util.concurrent.ConcurrentHashMap

@Serializable
data class CursorPosition(
    val fileIndex: Int = 0,
    val offset: Long = 0L
)

@Serializable
data class CursorState(
    val streams: MutableMap<String, CursorPosition> = ConcurrentHashMap()
)

class Cursor {

}