#pragma once

#include <string>
#include <chrono>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include "httplib.h"
#include "BlackBox.hpp"
#include <nlohmann/json.hpp>

class BlackBoxHttpServer {
public:
    BlackBoxHttpServer(std::string host, int port, std::string dataDir = "");
    void run();

private:
    void setupRoutes();

    struct Metrics {
        std::atomic<uint64_t> searchCount{0};
        std::atomic<uint64_t> searchLatencyMs{0};
        std::atomic<uint64_t> annCount{0};
        std::atomic<uint64_t> annLatencyMs{0};
        std::atomic<uint64_t> snapshotCount{0};
        std::atomic<uint64_t> snapshotLatencyMs{0};
        std::atomic<uint64_t> rateLimited{0};
        std::atomic<uint64_t> replayErrors{0};
        std::atomic<uint64_t> schemaMismatches{0};
    };

    std::string host_;
    int port_;
    httplib::Server server_;
    minielastic::BlackBox db_;
    std::string dataDir_;
    std::chrono::steady_clock::time_point startTime_;
    Metrics metrics_;
    int rateLimitQps_ = 0;
    std::mutex rateMutex_;
    std::unordered_map<std::string, std::pair<int64_t, int>> rateWindow_;
};
