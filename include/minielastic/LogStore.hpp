#pragma once

#include <cstdint>
#include <functional>
#include <fstream>
#include <string>
#include <nlohmann/json.hpp>
#include "minielastic/Checksum.hpp"

namespace minielastic {

struct LogRecord {
    enum class Op { Put, Del };
    Op op;
    uint32_t id;
    nlohmann::json doc;
};

// Append-only log with per-record checksums. Minimal durability layer.
class LogStore {
public:
    explicit LogStore(const std::string& dataDir);
    ~LogStore();

    // Replay all records; returns false on checksum/format failure.
    bool load(const std::function<void(const LogRecord&)>& onRecord);

    // Append a single record.
    void append(const LogRecord& record);

    bool good() const { return static_cast<bool>(stream_); }

private:
    std::string dataDir_;
    std::string logPath_;
    std::ofstream stream_;

    void ensureOpen();
};

} // namespace minielastic
