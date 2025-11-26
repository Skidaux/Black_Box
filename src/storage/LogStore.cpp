#include "minielastic/LogStore.hpp"

#include <filesystem>
#include <iostream>

using json = nlohmann::json;

namespace minielastic {

LogStore::LogStore(const std::string& dataDir) : dataDir_(dataDir) {
    std::filesystem::create_directories(dataDir_);
    logPath_ = (std::filesystem::path(dataDir_) / "data.log").string();
    ensureOpen();
}

LogStore::~LogStore() {
    if (stream_.is_open()) {
        stream_.flush();
        stream_.close();
    }
}

void LogStore::ensureOpen() {
    if (!stream_.is_open()) {
        stream_.open(logPath_, std::ios::binary | std::ios::app);
    }
}

bool LogStore::load(const std::function<void(const LogRecord&)>& onRecord) {
    std::ifstream in(logPath_, std::ios::binary);
    if (!in) return true; // nothing to load is not an error

    while (true) {
        uint32_t len = 0;
        if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
        if (len == 0 || len > (10 * 1024 * 1024)) {
            std::cerr << "LogStore: suspicious record length " << len << "\n";
            return false;
        }

        std::string payload(len, '\0');
        if (!in.read(payload.data(), len)) break;

        uint32_t storedCrc = 0;
        if (!in.read(reinterpret_cast<char*>(&storedCrc), sizeof(storedCrc))) break;

        if (crc32(payload) != storedCrc) {
            std::cerr << "LogStore: checksum mismatch; stopping replay\n";
            return false;
        }

        auto rec = json::parse(payload, nullptr, false);
        if (rec.is_discarded()) {
            std::cerr << "LogStore: invalid JSON record; skipping\n";
            continue;
        }

        LogRecord record;
        auto opStr = rec.value("op", "");
        if (opStr == "put") {
            record.op = LogRecord::Op::Put;
        } else if (opStr == "del") {
            record.op = LogRecord::Op::Del;
        } else {
            std::cerr << "LogStore: unknown op; skipping\n";
            continue;
        }
        record.id = rec.value("id", 0u);
        if (rec.contains("doc")) {
            record.doc = rec["doc"];
        }

        onRecord(record);
    }

    return true;
}

void LogStore::append(const LogRecord& record) {
    ensureOpen();
    if (!stream_) return;

    json rec = {
        {"op", record.op == LogRecord::Op::Put ? "put" : "del"},
        {"id", record.id}
    };
    if (record.op == LogRecord::Op::Put) {
        rec["doc"] = record.doc;
    }

    std::string payload = rec.dump();
    uint32_t len = static_cast<uint32_t>(payload.size());
    uint32_t checksum = crc32(payload);

    stream_.write(reinterpret_cast<const char*>(&len), sizeof(len));
    stream_.write(payload.data(), payload.size());
    stream_.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
    stream_.flush();
}

uint32_t LogStore::crc32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char b : data) {
        crc ^= b;
        for (int i = 0; i < 8; ++i) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

} // namespace minielastic
