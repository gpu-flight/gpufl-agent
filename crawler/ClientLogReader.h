#pragma once
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <sstream>
#include <optional>
#include <cstdint>

namespace gpumon {

class ClientLogReader {
public:
    explicit ClientLogReader(std::string logFilePath, bool debugMode);
    ~ClientLogReader();

    // Check if log file exists
    [[nodiscard]] bool isValid() const;

    // Read new lines since the last cursor position
    // Updates internal cursor but does not save to disk
    std::vector<std::string> readNewLogs();
    // Explicitly save cursor to disk (call this periodically)
    void saveCursor() const;

    [[nodiscard]] std::string getLogFilePath() const { return logFilePath_; }

private:
    std::string logFilePath_;
    std::string cursorFilePath_;
    bool debugMode_;
    std::streampos lastPosition_;

    void loadCursor();
};

}