#pragma once
#include <string>
#include <memory>

enum class Channel { HTTP, WS, MSG };

struct IMetricsSender {
    virtual ~IMetricsSender() = default;
    // Sends a JSON payload. Accepts either an object or an array of objects.
    virtual void send(const std::string& json) = 0;
};

// Factory that creates a sender bound to a backend base URL and API key.
// backendBase should be like "http://localhost:8080" (no trailing slash).
// apiKey will be attached as header X-API-Key to all requests.
std::unique_ptr<IMetricsSender> makeSender(Channel ch, const std::string& backendBase, const std::string& apiKey);
