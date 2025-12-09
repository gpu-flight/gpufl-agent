#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <sstream>
#include <filesystem>
#include "Utils.h"
#include "MetricsSender.h"
#include "GpuMonitor.h"
#include "Config.h"
#ifdef _WIN32
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <unistd.h>
#endif

namespace fs = std::filesystem;

static Channel channelFromEnv() {
    // Default to HTTP posting to /metrics
    return Channel::HTTP;
}

// Scan log directory for gpumon_*.log files
static std::vector<std::string> getClientLogPaths(const std::string& logDir) {
    std::vector<std::string> logPaths;

    if (logDir.empty()) {
        return logPaths;
    }

    // Scan the directory for all gpumon_*.log files
    if (fs::exists(logDir) && fs::is_directory(logDir)) {
        std::cout << "Scanning for gpumon logs in: " << logDir << std::endl;
        for (const auto& entry : fs::directory_iterator(logDir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".log") {
                // Only match gpumon_*.log files
                std::string filename = entry.path().filename().string();
                if (filename.find("gpumon_") == 0) {
                    logPaths.push_back(entry.path().string());
                }
            }
        }
    } else {
        std::cerr << "Warning: Log directory '" << logDir << "' does not exist or is not a directory" << std::endl;
    }

    return logPaths;
}

static std::string buildSelfTestSampleJson(const std::string& hostname) {
    // Minimal valid metric per backend expectations
    std::ostringstream j;
    j << "{";
    j << "\"timestamp\":\"" << util::nowIso8601Utc() << "\",";
    j << "\"metric\":\"power\",";
    j << "\"hostname\":\"" << util::escapeJson(hostname) << "\",";
    j << "\"gpuName\":\"GPU\",";
    j << "\"watts\":100.0";
    j << "}";
    return j.str();
}

int main(int argc, char** argv) {
    try {
        // Resolve settings
        Settings settings = config::resolveFromSources(argc, argv);

        if (settings.setKey) {
            // Force re-prompt and overwrite stored key
            settings.apiKey.clear();
            settings = config::interactiveSetup(settings);
            std::cout << "API key saved. Ingestion enabled." << std::endl;
            if (!settings.selfTest) return 0; // if not continuing with self-test
        }

        if (settings.apiKey.empty()) {
            // Interactive first run prompt
            settings = config::interactiveSetup(settings);
        }

        // Prepare sender
        auto sender = makeSender(channelFromEnv(), settings.backendUrl, settings.apiKey);

        if (settings.selfTest) {
            // Send a single metric and print response behavior (HTTP sender prints errors/status)
            std::string host = "host";
            char buf[256];
            if (gethostname(buf, sizeof(buf)) == 0) host = buf;
            std::string sample = buildSelfTestSampleJson(host);
            sender->send(sample);
            std::cout << "[OK] Self-test request sent." << std::endl;
            return 0;
        }

        // Normal run
        // Get clientlib log paths for enrichment
        std::vector<std::string> logPaths = getClientLogPaths(settings.logDirectory);
        if (!logPaths.empty()) {
            std::cout << "Will monitor " << logPaths.size() << " clientlib log file(s) for enrichment:" << std::endl;
            for (const auto& path : logPaths) {
                std::cout << "  - " << path << std::endl;
            }
        } else if (!settings.logDirectory.empty()) {
            std::cout << "Warning: No gpumon_*.log files found in " << settings.logDirectory << std::endl;
            std::cout << "Process metrics will not be enriched." << std::endl;
        } else {
            std::cout << "No log directory configured. Process metrics will not be enriched." << std::endl;
            std::cout << "To enable enrichment:" << std::endl;
            std::cout << "  - Set GPUMON_LOG_DIR environment variable" << std::endl;
            std::cout << "  - Or use --log-dir=/path/to/logs" << std::endl;
            std::cout << "  - Or edit config file to add logDirectory" << std::endl;
        }

        GpuMonitor monitor(std::move(sender), logPaths);
        monitor.runLoop();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << std::endl;
        return 1;
    }
}
