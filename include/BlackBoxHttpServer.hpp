#pragma once

#include <string>
#include <chrono>
#include "httplib.h"
#include "BlackBox.hpp"
#include <nlohmann/json.hpp>

class BlackBoxHttpServer {
public:
    BlackBoxHttpServer(std::string host, int port, std::string dataDir = "");
    void run();

private:
    void setupRoutes();

    std::string host_;
    int port_;
    httplib::Server server_;
    minielastic::BlackBox db_;
    std::string dataDir_;
    std::chrono::steady_clock::time_point startTime_;
};
