#include "BlackBoxHttpServer.hpp"

#include <filesystem>
#include <chrono>
#include <iostream>
#include <sstream>
#include <functional>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <optional>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

using json = nlohmann::json;

BlackBoxHttpServer::BlackBoxHttpServer(std::string host, int port, std::string dataDir)
    : host_(std::move(host)), port_(port), db_(dataDir), dataDir_(std::move(dataDir)), startTime_(std::chrono::steady_clock::now()) {
    setupRoutes();
}

void BlackBoxHttpServer::run() {
    // Configure thread pool size for request handling (default: hardware_concurrency or env override)
    int threads = static_cast<int>(std::max(4u, std::thread::hardware_concurrency()));
    if (const char* env = std::getenv("BLACKBOX_SERVER_THREADS")) {
        try { threads = std::max(1, std::stoi(env)); } catch (...) {}
    }
    server_.new_task_queue = [threads]() {
        return new httplib::ThreadPool(static_cast<size_t>(threads));
    };

    // Avoid Nagle delays on Linux; helps p99 latency on small requests
    server_.set_socket_options([](socket_t sock) {
        int flag = 1;
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&flag), sizeof(flag));
        return true;
    });

    std::cout << "BlackBox HTTP server listening on "
              << host_ << ":" << port_ << " with " << threads << " threads" << std::endl;
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
    auto resolveDocId = [this](const std::string& index, const std::string& raw) -> std::optional<minielastic::BlackBox::DocId> {
        return db_.lookupDocId(index, raw);
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

    // --- METRICS/STATS ---
    server_.Get("/v1/metrics", [this, ok, addCors](const httplib::Request&, httplib::Response& res) {
        auto now = std::chrono::steady_clock::now();
        double uptimeSec = std::chrono::duration<double>(now - startTime_).count();
        json data;
        data["uptime_seconds"] = uptimeSec;
        data["config"] = db_.config();
        auto stats = db_.stats();
        json jstats = json::array();
        std::size_t totalDocs = 0;
        std::size_t totalSegments = 0;
        std::size_t totalVectors = 0;
        uint64_t totalWal = 0;
        for (const auto& s : stats) {
            totalDocs += s.documents;
            totalSegments += s.segments;
            totalVectors += s.vectors;
            totalWal += s.walBytes;
            jstats.push_back({
                {"name", s.name},
                {"documents", s.documents},
                {"segments", s.segments},
                {"vectors", s.vectors},
                {"ann_clusters", s.annClusters},
                {"wal_bytes", s.walBytes},
                {"pending_ops", s.pendingOps},
                {"avg_doc_len", s.avgDocLen}
            });
        }
        data["totals"] = {
            {"documents", totalDocs},
            {"segments", totalSegments},
            {"vectors", totalVectors},
            {"wal_bytes", totalWal}
        };
        data["indexes"] = jstats;
        res.set_content(ok(data).dump(), "application/json");
        addCors(res);
    });

    // --- PROMETHEUS METRICS ---
    server_.Get("/metrics", [this](const httplib::Request&, httplib::Response& res) {
        auto now = std::chrono::steady_clock::now();
        double uptimeSec = std::chrono::duration<double>(now - startTime_).count();
        auto stats = db_.stats();
        std::size_t totalDocs = 0;
        std::size_t totalSegments = 0;
        std::size_t totalVectors = 0;
        uint64_t totalWal = 0;
        std::ostringstream out;
        out << "# HELP blackbox_uptime_seconds Uptime of the BlackBox server in seconds\n";
        out << "# TYPE blackbox_uptime_seconds gauge\n";
        out << "blackbox_uptime_seconds " << uptimeSec << "\n";

        out << "# HELP blackbox_indexes Number of indexes\n";
        out << "# TYPE blackbox_indexes gauge\n";
        out << "blackbox_indexes " << stats.size() << "\n";

        out << "# HELP blackbox_documents_total Total documents across all indexes\n";
        out << "# TYPE blackbox_documents_total gauge\n";
        for (const auto& s : stats) totalDocs += s.documents;
        out << "blackbox_documents_total " << totalDocs << "\n";

        out << "# HELP blackbox_segments_total Total segments across all indexes\n";
        out << "# TYPE blackbox_segments_total gauge\n";
        for (const auto& s : stats) totalSegments += s.segments;
        out << "blackbox_segments_total " << totalSegments << "\n";

        out << "# HELP blackbox_vectors_total Total vectors across all indexes\n";
        out << "# TYPE blackbox_vectors_total gauge\n";
        for (const auto& s : stats) totalVectors += s.vectors;
        out << "blackbox_vectors_total " << totalVectors << "\n";

        out << "# HELP blackbox_wal_bytes_total WAL bytes across all indexes\n";
        out << "# TYPE blackbox_wal_bytes_total gauge\n";
        for (const auto& s : stats) totalWal += s.walBytes;
        out << "blackbox_wal_bytes_total " << totalWal << "\n";

        out << "# HELP blackbox_index_documents Documents per index\n";
        out << "# TYPE blackbox_index_documents gauge\n";
        for (const auto& s : stats) {
            out << "blackbox_index_documents{index=\"" << s.name << "\"} " << s.documents << "\n";
        }
        out << "# HELP blackbox_index_segments Segments per index\n";
        out << "# TYPE blackbox_index_segments gauge\n";
        for (const auto& s : stats) {
            out << "blackbox_index_segments{index=\"" << s.name << "\"} " << s.segments << "\n";
        }
        out << "# HELP blackbox_index_wal_bytes WAL bytes per index\n";
        out << "# TYPE blackbox_index_wal_bytes gauge\n";
        for (const auto& s : stats) {
            out << "blackbox_index_wal_bytes{index=\"" << s.name << "\"} " << s.walBytes << "\n";
        }
        out << "# HELP blackbox_index_pending_ops Pending in-memory ops per index\n";
        out << "# TYPE blackbox_index_pending_ops gauge\n";
        for (const auto& s : stats) {
            out << "blackbox_index_pending_ops{index=\"" << s.name << "\"} " << s.pendingOps << "\n";
        }

        res.set_content(out.str(), "text/plain; version=0.0.4");
    });

    // --- CONFIG ---
    server_.Get("/v1/config", [this, ok, addCors](const httplib::Request&, httplib::Response& res) {
        res.set_content(ok(db_.config()).dump(), "application/json");
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

    // --- CUSTOM AGGREGATIONS ---
    server_.Get("/v1/custom", [this, ok, addCors](const httplib::Request&, httplib::Response& res) {
        auto list = db_.listCustomApis();
        res.set_content(ok(list).dump(), "application/json");
        addCors(res);
    });

    server_.Get(R"(/v1/custom/([^/]+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        auto spec = db_.getCustomApi(req.matches[1]);
        if (!spec) {
            res.status = 404;
            res.set_content(err(404, "Custom API not found").dump(), "application/json");
        } else {
            res.set_content(ok(*spec).dump(), "application/json");
        }
        addCors(res);
    });

    server_.Put(R"(/v1/custom/([^/]+))", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            auto spec = json::parse(req.body);
            if (!db_.createOrUpdateCustomApi(req.matches[1], spec)) {
                res.status = 400;
                res.set_content(err(400, "Invalid custom API spec").dump(), "application/json");
            } else {
                res.set_content(ok(json{{"name", req.matches[1]}}).dump(), "application/json");
            }
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON: ") + e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    server_.Delete(R"(/v1/custom/([^/]+))", [this, ok, err, addCors](const httplib::Request& req, httplib::Response& res) {
        if (!db_.removeCustomApi(req.matches[1])) {
            res.status = 404;
            res.set_content(err(404, "Custom API not found").dump(), "application/json");
        } else {
            res.set_content(ok(json{{"deleted", true}}).dump(), "application/json");
        }
        addCors(res);
    });

    server_.Post(R"(/v1/custom/([^/]+))", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        json params = json::object();
        if (!req.body.empty()) {
            if (!isJsonContent(req)) {
                res.status = 415;
                res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
                addCors(res);
                return;
            }
            try {
                params = json::parse(req.body);
            } catch (const std::exception& e) {
                res.status = 400;
                res.set_content(err(400, std::string("Invalid JSON: ") + e.what()).dump(), "application/json");
                addCors(res);
                return;
            }
        }
        try {
            auto result = db_.runCustomApi(req.matches[1], params);
            res.set_content(ok(result).dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    // --- LIST INDEXES ---
    server_.Get("/v1/indexes", [this, ok, addCors](const httplib::Request&, httplib::Response& res) {
        json arr = json::array();
        for (const auto& st : db_.stats()) {
            arr.push_back({
                {"name", st.name},
                {"documents", st.documents},
                {"segments", st.segments},
                {"vectors", st.vectors}
            });
        }
        res.set_content(ok(arr).dump(), "application/json");
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
            if (auto ext = db_.externalIdForDoc(index, id)) {
                response["doc_id"] = *ext;
            }
            res.status = 201;
            res.set_content(ok(response).dump(), "application/json");
        }
        catch (const std::exception& e) {
            std::cerr << "Index doc failed index=" << index << " err=" << e.what() << "\n";
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON or index: ") + e.what()).dump(), "application/json");
        }
        addCors(res);
    });

    // --- BULK INDEX ---
    server_.Post(R"(/v1/([^/]+)/_bulk)", [this, ok, err, isJsonContent, addCors](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        bool continueOnError = req.get_param_value("continue_on_error") != "false";
        json body;
        try {
            body = json::parse(req.body);
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON: ") + e.what()).dump(), "application/json");
            addCors(res);
            return;
        }
        const json* docsNode = nullptr;
        if (body.is_array()) {
            docsNode = &body;
        } else if (body.is_object() && body.contains("docs") && body["docs"].is_array()) {
            docsNode = &body["docs"];
        }
        if (!docsNode) {
            res.status = 400;
            res.set_content(err(400, "Body must be an array or an object with 'docs' array").dump(), "application/json");
            addCors(res);
            return;
        }
        std::vector<uint32_t> ids;
        std::vector<json> errors;
        ids.reserve(docsNode->size());
        for (size_t i = 0; i < docsNode->size(); ++i) {
            try {
                auto id = db_.indexDocument(index, (*docsNode)[i].dump());
                ids.push_back(id);
            } catch (const std::exception& e) {
                errors.push_back(json{{"index", i}, {"error", e.what()}});
                if (!continueOnError) break;
            }
        }
        json payload{
            {"indexed", ids.size()},
            {"ids", ids},
            {"errors", errors}
        };
        res.status = errors.empty() ? 201 : 207;
        res.set_content(ok(payload).dump(), "application/json");
        addCors(res);
    });

    // --- GET DOCUMENT ---
    server_.Get(R"(/v1/([^/]+)/doc/([^/]+))", [this, ok, err, addCors, resolveDocId](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        auto docIdOpt = resolveDocId(index, req.matches[2]);
        if (!docIdOpt) {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            auto doc = db_.getDocument(index, *docIdOpt);
            json data = {
                {"id", *docIdOpt},
                {"doc", doc}
            };
            if (auto ext = db_.externalIdForDoc(index, *docIdOpt)) {
                data["doc_id"] = *ext;
            }
            res.set_content(ok(data).dump(), "application/json");
        }
        catch (...) {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
        }
        addCors(res);
    });

    // --- UPDATE DOCUMENT (PUT = replace, PATCH = partial) ---
    auto updateDoc = [this, ok, err, isJsonContent, addCors, resolveDocId](const httplib::Request& req, httplib::Response& res, bool partial) {
        std::string index = req.matches[1];
        if (!isJsonContent(req)) {
            res.status = 415;
            res.set_content(err(415, "Content-Type must be application/json").dump(), "application/json");
            addCors(res);
            return;
        }
        auto docIdOpt = resolveDocId(index, req.matches[2]);
        if (!docIdOpt) {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
            addCors(res);
            return;
        }
        try {
            if (!db_.updateDocument(index, *docIdOpt, req.body, partial)) {
                res.status = 404;
                res.set_content(err(404, "Document not found").dump(), "application/json");
            } else {
                json payload = {{"id", *docIdOpt}};
                if (auto ext = db_.externalIdForDoc(index, *docIdOpt)) {
                    payload["doc_id"] = *ext;
                }
                res.set_content(ok(payload).dump(), "application/json");
            }
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(err(400, std::string("Invalid JSON or schema: ") + e.what()).dump(), "application/json");
        } catch (...) {
            res.status = 500;
            res.set_content(err(500, "update failed").dump(), "application/json");
        }
        addCors(res);
    };
    server_.Put(R"(/v1/([^/]+)/doc/([^/]+))", [updateDoc](const httplib::Request& req, httplib::Response& res){ updateDoc(req, res, false); });
    server_.Patch(R"(/v1/([^/]+)/doc/([^/]+))", [updateDoc](const httplib::Request& req, httplib::Response& res){ updateDoc(req, res, true); });

    // --- DELETE DOCUMENT ---
    server_.Delete(R"(/v1/([^/]+)/doc/([^/]+))", [this, ok, err, addCors, resolveDocId](const httplib::Request& req, httplib::Response& res) {
        std::string index = req.matches[1];
        auto docIdOpt = resolveDocId(index, req.matches[2]);
        if (!docIdOpt) {
            res.status = 404;
            res.set_content(err(404, "Document not found").dump(), "application/json");
            addCors(res);
            return;
        }

        auto existingExternal = db_.externalIdForDoc(index, *docIdOpt);
        if (db_.deleteDocument(index, *docIdOpt)) {
            json data = { {"id", *docIdOpt}, {"deleted", true} };
            if (existingExternal) {
                data["doc_id"] = *existingExternal;
            }
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
        if (!db_.indexExists(index)) {
            res.status = 404;
            res.set_content(err(404, "Index not found").dump(), "application/json");
            addCors(res);
            return;
        }
        auto q = req.get_param_value("q");
        auto mode = req.get_param_value("mode");
        if (mode.empty()) mode = "bm25";
        if (mode != "vector" && q.empty()) {
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
            if (mode == "vector") {
                auto vecStr = req.get_param_value("vec");
                if (vecStr.empty()) {
                    res.status = 400;
                    res.set_content(err(400, "Missing vector 'vec' parameter").dump(), "application/json");
                    addCors(res);
                    return;
                }
                std::vector<float> qvec;
                std::stringstream ss(vecStr);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    try { qvec.push_back(static_cast<float>(std::stof(item))); } catch (...) {}
                }
                results = db_.searchVector(index, qvec, need);
            } else if (mode == "hybrid") {
                results = db_.searchHybrid(index, q, wBm25, wSem, wLex, need);
            } else {
                if (q.empty()) {
                    res.status = 400;
                    res.set_content(err(400, "Missing query parameter 'q'").dump(), "application/json");
                    addCors(res);
                    return;
                }
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

        // Optional filters: tag, label, flag
        auto tagFilter = req.get_param_value("tag");
        auto labelFilter = req.get_param_value("label");
        auto flagParam = req.get_param_value("flag");
        bool flagHasFilter = !flagParam.empty();
        bool flagValue = flagParam == "true" || flagParam == "1";
        // schema-driven filters: filter_<field>=value, filter_<field>_min/_max for numbers, filter_<field>_bool
        const auto* schema = db_.getSchema(index);
        const auto* numVals = db_.getNumericValues(index);
        const auto* boolVals = db_.getBoolValues(index);
        const auto* strLists = db_.getStringLists(index);

        auto inStringList = [&](const std::string& field, const std::string& value, uint32_t docId)->bool {
            if (!strLists) return false;
            auto itField = strLists->find(field);
            if (itField == strLists->end()) return false;
            auto itBucket = itField->second.find(value);
            if (itBucket == itField->second.end()) return false;
            const auto& vec = itBucket->second;
            return std::find(vec.begin(), vec.end(), docId) != vec.end();
        };

        auto boolMatch = [&](const std::string& field, bool expected, uint32_t docId)->bool {
            if (!boolVals) return false;
            auto itField = boolVals->find(field);
            if (itField == boolVals->end()) return false;
            auto itVal = itField->second.find(docId);
            return itVal != itField->second.end() && itVal->second == expected;
        };

        // Apply filters before pagination
        std::vector<minielastic::BlackBox::SearchHit> filtered;
        filtered.reserve(results.size());
        for (const auto& hit : results) {
            bool passed = true;
            // Fast path: doc-values for tag/label/flag require doc fetch; we'll do it lazily
            json doc;
            auto ensureDoc = [&](bool needed) {
                if (!needed) return true;
                if (doc.is_null()) {
                    try { doc = db_.getDocument(index, hit.id); }
                    catch (...) { return false; }
                }
                return true;
            };

            if (!tagFilter.empty()) {
                bool okTag = inStringList("tags", tagFilter, hit.id);
                if (!okTag) {
                    if (!ensureDoc(true)) continue;
                    if (!(doc.contains("tags") && doc["tags"].is_array())) continue;
                    for (const auto& t : doc["tags"]) {
                        if (t.is_string() && t.get<std::string>() == tagFilter) { okTag = true; break; }
                    }
                }
                if (!okTag) continue;
            }
            if (!labelFilter.empty()) {
                bool okLabel = inStringList("labels", labelFilter, hit.id);
                if (!okLabel) {
                    if (!ensureDoc(true)) continue;
                    if (!(doc.contains("labels") && doc["labels"].is_array())) continue;
                    for (const auto& t : doc["labels"]) {
                        if (t.is_string() && t.get<std::string>() == labelFilter) { okLabel = true; break; }
                    }
                }
                if (!okLabel) continue;
            }
            if (flagHasFilter) {
                bool okFlag = boolMatch("flag", flagValue, hit.id);
                if (!okFlag) {
                    if (!ensureDoc(true)) continue;
                    if (!doc.contains("flag") || !doc["flag"].is_boolean() || doc["flag"].get<bool>() != flagValue) continue;
                }
            }

            if (schema) {
                for (const auto& ft : schema->fieldTypes) {
                    const auto& field = ft.first;
                    auto type = ft.second;
                    // Equals filter
                    auto eqParam = req.get_param_value("filter_" + field);
                    if (!eqParam.empty()) {
                        if (type == minielastic::BlackBox::FieldType::Bool && boolVals) {
                            bool target = eqParam == "true" || eqParam == "1";
                            auto itField = boolVals->find(field);
                            if (itField == boolVals->end() || itField->second.find(hit.id) == itField->second.end() || itField->second.at(hit.id) != target) { passed = false; break; }
                        } else if (type == minielastic::BlackBox::FieldType::Number && numVals) {
                            double target = std::stod(eqParam);
                            auto itField = numVals->find(field);
                            if (itField == numVals->end()) { passed = false; break; }
                            auto itVal = itField->second.find(hit.id);
                            if (itVal == itField->second.end() || itVal->second != target) { passed = false; break; }
                        } else if (type == minielastic::BlackBox::FieldType::ArrayString && strLists) {
                            auto itField = strLists->find(field);
                            bool found = false;
                            if (itField != strLists->end()) {
                                auto itBucket = itField->second.find(eqParam);
                                if (itBucket != itField->second.end()) {
                                    const auto& vec = itBucket->second;
                                    found = std::find(vec.begin(), vec.end(), hit.id) != vec.end();
                                }
                            }
                            if (!found) { passed = false; break; }
                        } else {
                            if (!ensureDoc(true)) { passed = false; break; }
                            if (!doc.contains(field) || !doc[field].is_string() || doc[field].get<std::string>() != eqParam) { passed = false; break; }
                        }
                    }
                    // Range filter for numbers
                    if (type == minielastic::BlackBox::FieldType::Number) {
                        auto minParam = req.get_param_value("filter_" + field + "_min");
                        auto maxParam = req.get_param_value("filter_" + field + "_max");
                        if (!minParam.empty() || !maxParam.empty()) {
                            double v = 0.0;
                            if (numVals) {
                                auto itField = numVals->find(field);
                                if (itField == numVals->end()) { passed = false; break; }
                                auto itVal = itField->second.find(hit.id);
                                if (itVal == itField->second.end()) { passed = false; break; }
                                v = itVal->second;
                            } else {
                                if (!ensureDoc(true)) { passed = false; break; }
                                if (!doc.contains(field) || !doc[field].is_number()) { passed = false; break; }
                                v = doc[field].get<double>();
                            }
                            if (!minParam.empty()) {
                                double mn = std::stod(minParam);
                                if (v < mn) { passed = false; break; }
                            }
                            if (!maxParam.empty()) {
                                double mx = std::stod(maxParam);
                                if (v > mx) { passed = false; break; }
                            }
                        }
                    }
                }
                if (!passed) continue;
            }
            filtered.push_back(hit);
        }

        size_t total = filtered.size();
        size_t start = std::min<size_t>(from, total);
        size_t end   = std::min<size_t>(start + size, total);

        auto relationModeParam = req.get_param_value("include_relations");
        std::string relationMode = relationModeParam.empty() ? "none" : relationModeParam;
        int relationDepth = parseBounded(req.get_param_value("max_relation_depth"), 1, 0, 5);

        struct RelationRef {
            std::string index;
            std::string id;
        };

        auto parseRelationFor = [this](const std::string& idxName, const json& document) -> std::optional<RelationRef> {
            const auto* relSchema = db_.getSchema(idxName);
            if (!relSchema || !relSchema->relation) return std::nullopt;
            const auto& cfg = *relSchema->relation;
            if (!document.contains(cfg.field) || document[cfg.field].is_null()) return std::nullopt;
            const auto& val = document[cfg.field];
            std::string targetIndex = cfg.targetIndex.empty() ? idxName : cfg.targetIndex;
            std::string relId;
            if (val.is_string()) {
                relId = val.get<std::string>();
            } else if (val.is_number_integer()) {
                relId = std::to_string(val.get<int64_t>());
            } else if (val.is_object()) {
                targetIndex = val.value("index", targetIndex);
                if (val.contains("id")) {
                    const auto& idNode = val["id"];
                    if (idNode.is_string()) relId = idNode.get<std::string>();
                    else if (idNode.is_number_integer()) relId = std::to_string(idNode.get<int64_t>());
                }
            }
            if (relId.empty()) return std::nullopt;
            return RelationRef{targetIndex, relId};
        };

        std::function<std::optional<json>(const RelationRef&, int, std::unordered_set<std::string>&)> resolveRelationDoc;
        resolveRelationDoc = [this, &parseRelationFor, &resolveRelationDoc, index](const RelationRef& ref, int depth, std::unordered_set<std::string>& visited) -> std::optional<json> {
            if (depth <= 0) return std::nullopt;
            std::string idxName = ref.index.empty() ? index : ref.index;
            auto idOpt = db_.lookupDocId(idxName, ref.id);
            if (!idOpt) return std::nullopt;
            std::string token = idxName + "::" + ref.id;
            if (!visited.insert(token).second) return std::nullopt;
            json doc;
            try {
                doc = db_.getDocument(idxName, *idOpt);
            } catch (...) {
                visited.erase(token);
                return std::nullopt;
            }
            json wrapper = {
                {"index", idxName},
                {"id", ref.id},
                {"doc", doc}
            };
            if (auto external = db_.externalIdForDoc(idxName, *idOpt)) {
                wrapper["doc_id"] = *external;
            }
            if (depth > 1) {
                if (auto nested = parseRelationFor(idxName, doc)) {
                    auto nestedDoc = resolveRelationDoc(*nested, depth - 1, visited);
                    if (nestedDoc) wrapper["relation"] = *nestedDoc;
                }
            }
            visited.erase(token);
            return wrapper;
        };

        struct MaterializedHit {
            minielastic::BlackBox::SearchHit hit;
            json doc;
            std::optional<RelationRef> relation;
        };
        std::vector<MaterializedHit> pageHits;
        pageHits.reserve(end - start);
        for (size_t i = start; i < end; ++i) {
            try {
                json doc = db_.getDocument(index, filtered[i].id);
                auto relation = parseRelationFor(index, doc);
                pageHits.push_back({filtered[i], doc, relation});
            } catch (...) {
                continue;
            }
        }

        json hits = json::array();
        json relationGroups = json::array();
        std::unordered_map<std::string, size_t> relationGroupIndex;

        auto relationKey = [&](const std::optional<RelationRef>& ref) {
            if (!ref) return std::string("__none__");
            std::string idxName = ref->index.empty() ? index : ref->index;
            return idxName + "::" + ref->id;
        };

        for (const auto& mh : pageHits) {
            json hit = {
                {"id", mh.hit.id},
                {"score", mh.hit.score},
                {"doc", mh.doc}
            };
            if (auto ext = db_.externalIdForDoc(index, mh.hit.id)) {
                hit["doc_id"] = *ext;
            }
            if ((relationMode == "inline" || relationMode == "hierarchy") && mh.relation) {
                std::unordered_set<std::string> visited;
                auto resolved = resolveRelationDoc(*mh.relation, relationDepth, visited);
                if (resolved) hit["relation"] = *resolved;
                json refJson = {
                    {"index", mh.relation->index.empty() ? index : mh.relation->index},
                    {"id", mh.relation->id}
                };
                hit["relation_ref"] = refJson;
            }
            hits.push_back(hit);

            if (relationMode == "hierarchy") {
                auto key = relationKey(mh.relation);
                size_t idxGroup = 0;
                if (relationGroupIndex.find(key) == relationGroupIndex.end()) {
                    json group;
                    if (mh.relation) {
                        std::unordered_set<std::string> visited;
                        auto resolved = resolveRelationDoc(*mh.relation, relationDepth, visited);
                        if (resolved) group["relation"] = *resolved;
                        group["relation_ref"] = {
                            {"index", mh.relation->index.empty() ? index : mh.relation->index},
                            {"id", mh.relation->id}
                        };
                    } else {
                        group["relation_ref"] = nullptr;
                    }
                    group["children"] = json::array();
                    relationGroups.push_back(group);
                    relationGroupIndex[key] = relationGroups.size() - 1;
                }
                idxGroup = relationGroupIndex[key];
                relationGroups[idxGroup]["children"].push_back(hit);
            }
        }

        json response = {
            {"query", q},
            {"from", from},
            {"size", size},
            {"total", total},
            {"mode", mode},
            {"hits", hits},
            {"relation_mode", relationMode}
        };
        if (!relationGroups.empty()) {
            response["relation_groups"] = relationGroups;
        }

        res.set_content(ok(response).dump(), "application/json");
        addCors(res);
    });
}
