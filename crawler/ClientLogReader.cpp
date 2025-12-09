#include "ClientLogReader.h"

#include <filesystem>
#include <iostream>
#include <utility>

#include "JsonUtils.h"

namespace gpumon {
    ClientLogReader::ClientLogReader(std::string logFilePath, const bool debugMode)
        : logFilePath_(std::move(logFilePath)),
          debugMode_(debugMode),
          lastPosition_(0) {

        // Cursor file is "logname.cursor" in the same directory
        cursorFilePath_ = logFilePath_ + ".cursor";
        loadCursor();
    }
    ClientLogReader::~ClientLogReader() {
        saveCursor();
    }

    bool ClientLogReader::isValid() const {
        return std::filesystem::exists(logFilePath_);
    }

    void ClientLogReader::loadCursor() {
        if (!std::filesystem::exists(cursorFilePath_)) {
            lastPosition_ = 0;
            return;
        }
        std::ifstream f(cursorFilePath_);
        if (f.is_open()) {
            long long pos = 0;
            f >> pos;
            lastPosition_ = pos;
            if (debugMode_) std::cout << "Loaded cursor for " << logFilePath_ << ": " << pos << std::endl;
        }
    }

    void ClientLogReader::saveCursor() const {
        std::ofstream f(cursorFilePath_, std::ios::trunc);
        if (f.is_open()) {
            f << lastPosition_;
            f.flush();
        }
    }

    std::vector<std::string> ClientLogReader::readNewLogs() {
        std::vector<std::string> newLines;
        std::ifstream file(logFilePath_, std::ios::binary);
        if (!file.is_open()) return newLines;

        // Check for rotation (File shrunk)
        file.seekg(0, std::ios::end);

        if (std::streampos currentSize = file.tellg();
            currentSize < lastPosition_) {
            if (debugMode_) std::cout << "Log rotation detected. Resetting cursor to 0." << std::endl;
            lastPosition_ = 0;
        }

        // Seek to last read position
        file.seekg(lastPosition_);

        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty()) {
                newLines.push_back(line);
            }
        }

        // Clear EOF flag to get accurate tellg
        if (file.eof()) file.clear();

        lastPosition_ = file.tellg();
        return newLines;
    }
}
