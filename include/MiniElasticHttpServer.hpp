#pragma once

#include <string>
#include "httplib.h"
#include "MiniElastic.hpp"
#include <nlohmann/json.hpp>

class MiniElasticHttpServer {
public:
    MiniElasticHttpServer(std::string host, int port);
    void run();

private:
    void setupRoutes();

    std::string host_;
    int port_;
    httplib::Server server_;
    minielastic::MiniElastic db_;
};
