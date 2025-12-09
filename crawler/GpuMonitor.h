#pragma once
#include <string>
#include <memory>
#include <stdexcept>
#include <atomic>
#include <vector>
#include "ClientLogReader.h"
#include "MetricsSender.h"
#include <thread>
#include "ThreadSafeQueue.h"
#include <cstddef>

class GpuMonitor {
public:
    explicit GpuMonitor(std::unique_ptr<IMetricsSender> sender, const std::vector<std::string>& clientLogPaths = {});
    ~GpuMonitor();

    void runLoop() const;

    static void requestStop();
    static bool isStopRequested();

private:
    void initializeLogReaders(std::vector<std::unique_ptr<gpumon::ClientLogReader>>& readers) const;
    void workerLoop() const;
    void sendLogEvent(const std::string& rawJson) const;

    static std::atomic<bool> stop_;
    std::unique_ptr<IMetricsSender> sender_;
    std::string hostname_;
    std::vector<std::string> clientLogPaths_;
    bool debugMode_;
    // Producer-Consumer components
    std::size_t queueCapacity_ {0};
    mutable std::unique_ptr<ThreadSafeQueue<std::string>> queue_;
    mutable std::thread worker_;
};