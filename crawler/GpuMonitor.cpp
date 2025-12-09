#include "GpuMonitor.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdlib>

#ifdef _WIN32
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <unistd.h>
#endif

// ============================================================================
// Helpers
// ============================================================================

std::atomic<bool> GpuMonitor::stop_{false};

static std::string getHostName() {
    char host[256];
    if (gethostname(host, sizeof(host)) == 0) return host;
    return "unknown";
}

// ============================================================================
// GpuMonitor Implementation
// ============================================================================

GpuMonitor::GpuMonitor(std::unique_ptr<IMetricsSender> sender, const std::vector<std::string>& clientLogPaths)
    : sender_(std::move(sender)), clientLogPaths_(clientLogPaths) {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
    hostname_ = getHostName();
    const char* dbg = std::getenv("GPUMON_DEBUG");
    debugMode_ = (dbg && (std::string(dbg) == "1" || std::string(dbg) == "true"));
    // Configure bounded queue capacity from env (default 10000, 0 = unbounded)
    std::size_t cap = 10000;
    if (const char* envCap = std::getenv("GPUMON_QUEUE_CAPACITY")) {
        try {
            long long parsed = std::stoll(envCap);
            if (parsed <= 0) cap = 0; // unbounded if non-positive
            else cap = static_cast<std::size_t>(parsed);
        } catch (...) {
            // keep default
        }
    }
    queueCapacity_ = cap;
    // Create bounded queue instance
    queue_ = std::make_unique<ThreadSafeQueue<std::string>>(queueCapacity_);
    // Start consumer worker thread
    worker_ = std::thread([this]{ this->workerLoop(); });
}

GpuMonitor::~GpuMonitor() {
    // Signal stop and close the queue to unblock consumer
    requestStop();
    if (queue_) queue_->close();
    if (worker_.joinable()) worker_.join();
#ifdef _WIN32
    WSACleanup();
#endif
}

void GpuMonitor::requestStop() { stop_.store(true, std::memory_order_relaxed); }
bool GpuMonitor::isStopRequested() { return stop_.load(std::memory_order_relaxed); }

void GpuMonitor::initializeLogReaders(std::vector<std::unique_ptr<gpumon::ClientLogReader>>& readers) const {
    for (const auto& logPath : clientLogPaths_) {
        // Create reader (it automatically loads the cursor)
        auto reader = std::make_unique<gpumon::ClientLogReader>(logPath, debugMode_);
        if (reader->isValid()) {
            if (debugMode_) std::cout << "Monitoring: " << logPath << std::endl;
            readers.push_back(std::move(reader));
        } else if (debugMode_) {
            std::cerr << "Skipping invalid log: " << logPath << std::endl;
        }
    }
}

void GpuMonitor::sendLogEvent(const std::string& rawJson) const {
    // Strategy: We want to send the raw JSON, but we MUST add the hostname
    // so the backend knows where it came from.
    // Efficient String Manipulation: Insert "hostname":"...", after the first '{'

    if (rawJson.empty() || rawJson[0] != '{') return;

    std::string packet = rawJson;
    std::string hostField = "\"hostname\":\"" + hostname_ + "\",";

    // Insert right after '{'
    packet.insert(1, hostField);

    if (debugMode_) std::cout << "[SEND] " << packet << std::endl;
    if (sender_) sender_->send(packet);
}

void GpuMonitor::runLoop() const {
    std::vector<std::unique_ptr<gpumon::ClientLogReader>> logReaders;
    initializeLogReaders(logReaders);

    if (logReaders.empty()) {
        std::cerr << "No valid logs found. Exiting." << std::endl;
        return;
    }

    while (!isStopRequested()) {
        bool anyData = false;

        for (const auto& r : logReaders) {
            // 1. Read Raw Lines (Cursor handles state)
            std::vector<std::string> lines = r->readNewLogs();

            if (!lines.empty()) {
                anyData = true;
                // 2. Enqueue lines for the consumer
                for (auto& line : lines) {
                    queue_->push(std::move(line));
                }

                // 3. Save cursor immediately after a batch is processed
                // This ensures we don't re-send if we crash 1 second later
                r->saveCursor();
            }
        }

        // Sleep to prevent CPU spinning
        // If we found data, sleep less (catch up), otherwise sleep more.
        if (anyData) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
    // On exit signal, close the queue so worker can finish
    if (queue_) queue_->close();
}

void GpuMonitor::workerLoop() const {
    std::string line;
    while (true) {
        if (!queue_->pop(line)) break; // closed and drained
        sendLogEvent(line);
    }
}