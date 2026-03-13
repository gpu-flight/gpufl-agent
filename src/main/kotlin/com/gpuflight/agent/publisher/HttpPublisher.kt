package com.gpuflight.com.gpuflight.agent.publisher

import com.gpuflight.com.gpuflight.agent.config.HttpConfig
import com.gpuflight.com.gpuflight.agent.config.JsonSettings.json
import com.gpuflight.com.gpuflight.agent.model.LogWrapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class HttpPublisher(
    private val config: HttpConfig,
    private val authToken: String? = null // Optional Bearer token
) : Publisher {

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1) // Compatible with more backends
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    override suspend fun publish(topic: String, key: String, log: LogWrapper) {
        withContext(Dispatchers.IO) {
            val url = config.endpointUrl + log.type
            val message = json.encodeToString(log)

            val requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("X-Topic", topic)
                .header("X-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(message))

            if (authToken != null) {
                requestBuilder.header("Authorization", "Bearer $authToken")
            }

            try {
                val response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())

                if (response.statusCode() !in 200..299) {
                    println("HTTP Publish failed [$url]: ${response.statusCode()} - ${response.body()}")
                }
            } catch (e: Exception) {
                println("HTTP Connection error [$url]: ${e.toString()}")
            }
        }
    }

    override fun close() {
        // Java HttpClient cleans itself up, nothing specific to close.
    }
}