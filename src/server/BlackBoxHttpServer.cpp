#include "BlackBoxHttpServer.hpp"

#include <filesystem>
#include <iostream>
#include <sstream>

using json = nlohmann::json;

BlackBoxHttpServer::BlackBoxHttpServer(std::string host, int port, std::string dataDir)
    : host_(std::move(host)), port_(port), db_(dataDir), dataDir_(std::move(dataDir)) {
    setupRoutes();
}

void BlackBoxHttpServer::run() {
    std::cout << "BlackBox HTTP server listening on "
              << host_ << ":" << port_ << std::endl;
    server_.listen(host_.c_str(), port_);
}

void BlackBoxHttpServer::setupRoutes() {

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
        res.set_content(ok(json{}).dump(), "application/json");
        addCors(res);
    });

    // --- CREATE INDEX ---
    server_.Post("/v1/indexes", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            auto j = json::parse(req.body);
            auto name = j.value("name", "");
            if (name.empty()) {
                res.status = 400;
                res.set_content(err(400, "Missing index name").dump(), "application/json");
                addCors(res);
                return;
            }
            minielastic::BlackBox::IndexSchema schema{j.value("schema", json::object())};
            if (!db_.createIndex(name, schema)) {
                res.status = 400;
                res.set_content(err(400, "Index already exists or invalid").dump(), "application/json");
                addCors(res);
                return;
            }
            res.status = 201;
            res.set_content(ok(json{{"name", name}}).dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON: ") + e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    // --- SNAPSHOT SAVE ---
    server_.Post("/v1/snapshot", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string overridePath = req.get_param_value("path");
        std::filesystem::path snapshotPath = overridePath.empty()
            ? (std::filesystem::path(dataDir_).empty() ? std::filesystem::path("index.manifest") : std::filesystem::path(dataDir_) / "index.manifest")
            : std::filesystem::path(overridePath);

        bool saved = db_.saveSnapshot(snapshotPath.string());
        if (saved) {
            json data = { {"path", snapshotPath.string()} };
            res.set_content(ok(data).dump(), "application/json");
        } else {
            res.status = 500;
            res.set_content(err(500, "Failed to write snapshot").dump(), "application/json");
        }
        addCors(res);
    });

    // --- SNAPSHOT LOAD ---
    server_.Post("/v1/snapshot/load", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string overridePath = req.get_param_value("path");
        std::filesystem::path snapshotPath = overridePath.empty()
            ? (std::filesystem::path(dataDir_).empty() ? std::filesystem::path("index.manifest") : std::filesystem::path(dataDir_) / "index.manifest")
            : std::filesystem::path(overridePath);

        bool loaded = db_.loadSnapshot(snapshotPath.string());
        if (loaded) {
            res.set_content(ok(json{{"path", snapshotPath.string()}}).dump(), "application/json");
        } else {
            res.status = 500;
            res.set_content(err(500, "Failed to load snapshot").dump(), "application/json");
        }
        addCors(res);
    });

    // --- INDEX DOCUMENT ---
    server_.Post(R"(/v1/([^/]+)/doc)", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            auto j = json::parse(req.body);
            auto id = db_.indexDocument(index, j.dump());
            json response = {{ "id", id }};
            res.status = 201;
            res.set_content(ok(response).dump(), "application/json");
        }
        catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON or index: ") + e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    // --- GET DOCUMENT ---
    server_.Get(R"(/v1/([^/]+)/doc/(\d+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        try {
            auto id = static_cast<minielastic::BlackBox::DocId>(std::stoul(req.matches[2]));
            auto doc = db_.getDocument(index, id);

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
    server_.Delete(R"(/v1/([^/]+)/doc/(\d+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        minielastic::BlackBox::DocId id{};
        try {
            id = static_cast<minielastic::BlackBox::DocId>(std::stoul(req.matches[2]));
        }
        catch (...) {
            res.status = 400;
            res.set_content(err(400, "Invalid document id").dump(), "application/json");
            addCors(res);
            return;
        }

        if (db_.deleteDocument(index, id)) {
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
    server_.Get(R"(/v1/([^/]+)/search)", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
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
        int maxDist = parseBounded(req.get_param_value("distance"), 1, 0, 3);
        auto mode = req.get_param_value("mode");
        if (mode.empty()) mode = "bm25";

        // Hybrid weights
        auto parseDouble = [](const std::string& v, double def) {
            if (v.empty()) return def;
            try { return std::stod(v); } catch (...) { return def; }
        };
        double wBm25 = parseDouble(req.get_param_value("w_bm25"), 1.0);
        double wSem  = parseDouble(req.get_param_value("w_semantic"), 1.0);
        double wLex  = parseDouble(req.get_param_value("w_lexical"), 0.5);

        size_t need = static_cast<size_t>(from + size);
        std::vector<minielastic::BlackBox::SearchHit> results;
        try {
            if (mode == "hybrid") {
                results = db_.searchHybrid(index, q, wBm25, wSem, wLex, need);
            } else {
                results = db_.search(index, q, mode, need, maxDist);
            }
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(err(500, std::string("search failed: ") + e.what()).dump(), "application/json");
            addCors(res);
            return;
        } catch (...) {
            res.status = 500;
            res.set_content(err(500, "search failed").dump(), "application/json");
            addCors(res);
            return;
        }

        size_t total = results.size();
        size_t start = std::min<size_t>(from, total);
        size_t end   = std::min<size_t>(start + size, total);

        json hits = json::array();
        for (size_t i = start; i < end; ++i) {
            auto id = results[i].id;
            hits.push_back({
                {"id", id},
                {"score", results[i].score},
                {"doc", db_.getDocument(index, id)}
            });
        }

        json response = {
            {"query", q},
            {"from", from},
            {"size", size},
            {"total", total},
            {"mode", mode},
            {"hits", hits}
        };

        res.set_content(ok(response).dump(), "application/json");
        addCors(res);
    });
}
