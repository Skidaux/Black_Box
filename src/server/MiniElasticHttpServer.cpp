#include "MiniElasticHttpServer.hpp"

#include <iostream>
#include <sstream>

using json = nlohmann::json;

MiniElasticHttpServer::MiniElasticHttpServer(std::string host, int port, std::string dataDir)
    : host_(std::move(host)), port_(port), db_(std::move(dataDir)) {
    setupRoutes();
}

void MiniElasticHttpServer::run() {
    std::cout << "MiniElastic HTTP server listening on "
              << host_ << ":" << port_ << std::endl;
    server_.listen(host_.c_str(), port_);
}

void MiniElasticHttpServer::setupRoutes() {

    // JSON helpers
    auto ok = [](const json& data) {
        return json{
            {"status", "ok"},
            {"data", data}
        };
    };

    auto err = [](int code, const std::string& message) {
        return json{
            {"status", "error"},
            {"error", {
                {"code", code},
                {"message", message}
            }}
        };
    };

    // JSON content checker
    auto isJsonContent = [](const httplib::Request& req) {
        auto ct = req.get_header_value("Content-Type");
        return ct.find("application/json") != std::string::npos;
    };



    // CORS helper to add to ALL responses
    auto addCors = [](httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type");
    };

    // Handle preflight OPTIONS requests for ANY route
    server_.Options(R"(.*)", [addCors](const httplib::Request&, httplib::Response& res) {
        addCors(res);
        res.status = 200;
        res.set_content("", "text/plain");
    });

    // --- HEALTH ---
    server_.Get("/v1/health", [this, ok, addCors](const httplib::Request&, httplib::Response& res) {
        json data = { {"docs", db_.documentCount()} };
        res.set_content(ok(data).dump(), "application/json");
        addCors(res);
    });

    // --- INDEX DOCUMENT ---
    server_.Post("/v1/index", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            auto j = json::parse(req.body);
            auto id = db_.indexDocument(j.dump());
            json response = {{ "id", id }};
            res.status = 201;
            res.set_content(ok(response).dump(), "application/json");
        }
        catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON: ") + e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    // --- GET DOCUMENT ---
    server_.Get(R"(/v1/doc/(\d+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        try {
            auto id = static_cast<minielastic::MiniElastic::DocId>(std::stoul(req.matches[1]));
            auto doc = db_.getDocument(id);

            json data = {
                {"id", id},
                {"doc", doc}
            };
            res.set_content(ok(data).dump(), "application/json");
        }
        catch (...) {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
        }
        addCors(res);
    });

    // --- DELETE DOCUMENT ---
    server_.Delete(R"(/v1/doc/(\d+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        minielastic::MiniElastic::DocId id{};
        try {
            id = static_cast<minielastic::MiniElastic::DocId>(std::stoul(req.matches[1]));
        }
        catch (...) {
            res.status = 400;
            res.set_content(err(400, "Invalid document id").dump(), "application/json");
            addCors(res);
            return;
        }

        if (db_.deleteDocument(id)) {
            json data = { {"id", id}, {"deleted", true} };
            res.set_content(ok(data).dump(), "application/json");
        }
        else {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
        }
        addCors(res);
    });

    // --- SEARCH ---
    server_.Get("/v1/search", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        auto q = req.get_param_value("q");
        if (q.empty()) {
            res.status = 400;
            res.set_content(err(400, "Missing query parameter 'q'").dump(), "application/json");
            addCors(res);
            return;
        }

        auto parseBounded = [](const std::string& val, int def, int min, int max) -> int {
            if (val.empty()) return def;
            try {
                int v = std::stoi(val);
                return std::min(std::max(v, min), max);
            }
            catch (...) { return def; }
        };

        int from = parseBounded(req.get_param_value("from"), 0, 0, 1'000'000);
        int size = parseBounded(req.get_param_value("size"), 10, 1, 500);

        auto ids = db_.search(q);

        size_t total = ids.size();
        size_t start = std::min<size_t>(from, total);
        size_t end   = std::min<size_t>(start + size, total);

        json hits = json::array();
        for (size_t i = start; i < end; ++i) {
            auto id = ids[i];
            hits.push_back({
                {"id", id},
                {"doc", db_.getDocument(id)}
            });
        }

        json response = {
            {"query", q},
            {"from", from},
            {"size", size},
            {"total", total},
            {"hits", hits}
        };

        res.set_content(ok(response).dump(), "application/json");
        addCors(res);
    });
}
