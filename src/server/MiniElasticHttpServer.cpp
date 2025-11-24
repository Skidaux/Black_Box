#include "MiniElasticHttpServer.hpp"

using json = nlohmann::json;

MiniElasticHttpServer::MiniElasticHttpServer(std::string host, int port)
    : host_(std::move(host)), port_(port) {
    setupRoutes();
}

void MiniElasticHttpServer::run() {
    std::cout << "MiniElastic HTTP server listening on "
              << host_ << ":" << port_ << std::endl;
    server_.listen(host_.c_str(), port_);
}

void MiniElasticHttpServer::setupRoutes() {

    server_.Get("/health", [this](const httplib::Request&, httplib::Response& res) {
        json j = {
            {"status", "ok"},
            {"docs", db_.documentCount()}
        };
        res.set_content(j.dump(), "application/json");
    });

    server_.Post("/index", [this](const httplib::Request& req, httplib::Response& res) {
        try {
            auto j = json::parse(req.body);
            auto id = db_.indexDocument(j.dump());
            json response = { {"id", id} };

            res.status = 201;
            res.set_content(response.dump(), "application/json");
        }
        catch (const std::exception& e) {
            json err = {{ "error", "Invalid JSON" }, { "what", e.what() }};
            res.status = 400;
            res.set_content(err.dump(), "application/json");
        }
    });

    server_.Get("/search", [this](const httplib::Request& req, httplib::Response& res) {
        auto q = req.get_param_value("q");

        if (q.empty()) {
            res.status = 400;
            res.set_content(R"({"error": "Missing 'q'"})", "application/json");
            return;
        }

        auto ids = db_.search(q);

        json response;
        response["query"] = q;
        response["hits"] = json::array();

        for (auto id : ids) {
            response["hits"].push_back({
                {"id", id},
                {"doc", db_.getDocument(id)}
            });
        }

        res.set_content(response.dump(), "application/json");
    });
}
