#include "Config.h"
#include "Utils.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#endif

using namespace std;

namespace fs = std::filesystem;

static string trim(const string& s){
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of(" \t\r\n");
    if (a==string::npos) return "";
    return s.substr(a, b-a+1);
}

static string extractJsonString(const string& content, const string& key) {
    // Extremely small JSON extractor: looks for "key":"value"
    string needle = string("\"") + key + "\"";
    size_t k = content.find(needle);
    if (k == string::npos) return "";
    size_t colon = content.find(':', k + needle.size());
    if (colon == string::npos) return "";
    size_t firstQuote = content.find('"', colon + 1);
    if (firstQuote == string::npos) return "";
    size_t secondQuote = content.find('"', firstQuote + 1);
    if (secondQuote == string::npos) return "";
    return content.substr(firstQuote + 1, secondQuote - firstQuote - 1);
}

namespace config {

string configPath() {
#ifdef _WIN32
    const char* appdata = std::getenv("APPDATA");
    string base = appdata ? string(appdata) : string(".");
    return base + "\\gpu-crawler\\config.json";
#else
    const char* home = std::getenv("HOME");
    string base = home ? string(home) : string(".");
    return base + "/.gpu-crawler/config.json";
#endif
}

Settings load() {
    Settings s;
    s.backendUrl = "";
    s.apiKey = "";
    s.logDirectory = "";

    ifstream f(configPath(), ios::in | ios::binary);
    if (!f) return s;
    std::ostringstream oss; oss << f.rdbuf();
    string content = oss.str();
    s.backendUrl = extractJsonString(content, "backendUrl");
    s.apiKey     = extractJsonString(content, "apiKey");
    s.logDirectory = extractJsonString(content, "logDirectory");
    return s;
}

void save(const Settings& s) {
    const string path = configPath();
    fs::path p(path);
    fs::create_directories(p.parent_path());
    ofstream f(path, ios::out | ios::trunc | ios::binary);
    if (!f) {
        cerr << "[WARN] Failed to write config file at " << path << endl;
        return;
    }
    f << "{\n";
    f << "  \"backendUrl\": \"" << util::escapeJson(s.backendUrl) << "\",\n";
    f << "  \"apiKey\": \"" << util::escapeJson(s.apiKey) << "\"";
    if (!s.logDirectory.empty()) {
        f << ",\n  \"logDirectory\": \"" << util::escapeJson(s.logDirectory) << "\"";
    }
    f << "\n}\n";
}

static bool argEq(const string& a, const char* b){ return a == b; }

Settings resolveFromSources(int argc, char** argv) {
    Settings file = load();

    Settings s;
    // 1) Start with config file
    s.backendUrl = file.backendUrl.empty() ? util::getenvOr("GPU_BACKEND_URL", "http://localhost:8080") : file.backendUrl;
    s.apiKey     = file.apiKey;
    s.logDirectory = file.logDirectory;

    // 2) Env override
    {
        string envUrl = util::getenvOr("GPU_BACKEND_URL", "");
        if (!envUrl.empty()) s.backendUrl = envUrl;
        string envKey = util::getenvOr("GPU_API_KEY", "");
        if (!envKey.empty()) s.apiKey = envKey;
        string envLogDir = util::getenvOr("GPUMON_LOG_DIR", "");
        if (!envLogDir.empty()) s.logDirectory = envLogDir;
    }

    // 3) CLI overrides
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (argEq(arg, "--backend-url") && i + 1 < argc) {
            s.backendUrl = argv[++i];
        } else if (arg.rfind("--backend-url=", 0) == 0) {
            s.backendUrl = arg.substr(strlen("--backend-url="));
        } else if (argEq(arg, "--api-key") && i + 1 < argc) {
            s.apiKey = argv[++i];
        } else if (arg.rfind("--api-key=", 0) == 0) {
            s.apiKey = arg.substr(strlen("--api-key="));
        } else if (argEq(arg, "--log-dir") && i + 1 < argc) {
            s.logDirectory = argv[++i];
        } else if (arg.rfind("--log-dir=", 0) == 0) {
            s.logDirectory = arg.substr(strlen("--log-dir="));
        } else if (argEq(arg, "--self-test")) {
            s.selfTest = true;
        } else if (argEq(arg, "--set-key")) {
            s.setKey = true;
        }
    }

    return s;
}

static string prompt(const string& message, const string& defaultVal = ""){
    if (!defaultVal.empty()) {
        cout << message << " [default: " << defaultVal << "]: ";
    } else {
        cout << message << ": ";
    }
    cout.flush();
    string s; getline(cin, s);
    if (s.empty()) return defaultVal;
    return trim(s);
}

Settings interactiveSetup(const Settings& base) {
    Settings s = base;

    cout << "Welcome to GPU Crawler." << endl;
    cout << endl;
    cout << "Your backend requires an API key for ingestion." << endl;
    cout << endl;
    cout << "1) Please paste your API key (from registration response or Account page):\n   (We will store it locally for future runs.)" << endl;

    if (s.apiKey.empty()) {
        string key = prompt("API key");
        if (key.empty()) {
            cerr << "[ERROR] Empty API key." << endl;
            std::exit(2);
        }
        s.apiKey = key;
    } else {
        cout << "Using existing API key from env/config." << endl;
    }

    string backendDefault = s.backendUrl.empty() ? string("http://localhost:8080") : s.backendUrl;
    string backend = prompt("\n2) Backend URL", backendDefault);
    if (backend.empty()) backend = backendDefault;
    s.backendUrl = backend;

    // Ask for log directory
    if (s.logDirectory.empty()) {
        cout << "\n3) Client log directory (where gpumon_*.log files are written):" << endl;
        cout << "   Leave empty to skip process-level enrichment." << endl;
        string logDir = prompt("Log directory", "");
        s.logDirectory = logDir;
    }

    cout << "\nWe will send metrics to: " << s.backendUrl << "/metrics with header:\n  X-API-Key: <YOUR_KEY>" << endl;
    if (!s.logDirectory.empty()) {
        cout << "Process enrichment enabled from: " << s.logDirectory << endl;
    }

    // Save settings
    save(s);

    // Optional self-test question only if not already set
    if (!s.selfTest) {
        string ans = prompt("\nOptional self-test now? This will send a minimal metric sample.\n  [Y/n]", "Y");
        if (!ans.empty() && (ans[0] == 'Y' || ans[0] == 'y')) {
            s.selfTest = true;
        }
    }

    return s;
}

} // namespace config
