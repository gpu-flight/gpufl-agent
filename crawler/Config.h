#pragma once
#include <string>

struct Settings {
    std::string backendUrl; // e.g., http://localhost:8080
    std::string apiKey;     // API key for X-API-Key
    std::string logDirectory; // Directory to scan for gpumon_*.log files
    bool selfTest = false;
    bool setKey = false;
};

namespace config {
// Determine platform-specific config path
std::string configPath();

// Load existing config file if present; returns empty strings if missing
Settings load();

// Save apiKey and backendUrl to the config file (creates directories as needed)
void save(const Settings& s);

// Resolve settings from CLI > env > config
// - argv: vector of args starting from argv[1]
Settings resolveFromSources(int argc, char** argv);

// Interactively prompt for missing api key and optionally backend.
// Returns updated settings and persists them.
Settings interactiveSetup(const Settings& base);
} // namespace config
