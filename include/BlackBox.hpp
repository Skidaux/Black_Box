//BlackBox.hpp
#pragma once

#include <cstdint>
#include <fstream>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <unordered_set>
#include <atomic>
#include <list>
#include <deque>
#include <unordered_set>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <nlohmann/json.hpp>
#include "minielastic/Analyzer.hpp"
#include "minielastic/algorithms/SearchAlgorithms.hpp"

namespace minielastic {

using json = nlohmann::json;

template <typename T>
inline void writeLE(std::ostream& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        char byte = static_cast<char>((static_cast<uint64_t>(value) >> (8 * i)) & 0xFFu);
        out.write(&byte, 1);
    }
}

template <typename T>
inline void readLE(std::istream& in, T& value) {
    value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        char byte = 0;
        if (!in.read(&byte, 1)) return;
        value |= static_cast<uint64_t>(static_cast<unsigned char>(byte)) << (8 * i);
    }
}

    enum class WalOp : uint8_t { Upsert = 1, Delete = 2 };

    struct WalRecord {
        WalOp op;
        uint64_t opId = 0;
        uint32_t docId = 0;
        std::vector<char> payload; // CBOR data
    };

    struct WalWriter {
        std::string path;
        std::string schemaId;
        std::ofstream stream;
        uint64_t offset = 0;
        uint64_t pendingBytes = 0;
        uint64_t flushThresholdBytes = 64 * 1024;
        std::chrono::steady_clock::time_point lastFlush = std::chrono::steady_clock::now();
        std::chrono::milliseconds flushInterval{200};
        bool enableFsync = true;
        bool legacyFormat = false;
        uint16_t fileVersion = 0;
        uint16_t fileFlags = 0;
        uint64_t headerBytes = 0;
        std::string headerSchemaId;
        bool upgradedFromLegacy = false;
        bool schemaMismatch = false;

        WalWriter() = default;
        explicit WalWriter(std::string p) : path(std::move(p)) {}

        bool open();
        void close();
        bool append(const WalRecord& rec);
        void maybeFlush(bool force = false);
        void reset();
    };

    std::vector<WalRecord> readWalRecords(const std::string& path, uint64_t startOffset = 0);

} // namespace minielastic
namespace minielastic {

class BlackBox {
public:
    using DocId = uint32_t;
    using SearchHit = algo::SearchHit;

    enum class FieldType { Text, ArrayString, Bool, Number, Vector, Image, QueryValues, Unknown };

    struct IndexSchema {
        nlohmann::json schema;
        uint32_t vectorDim = 0; // optional vector dimension for vector field
        std::unordered_map<std::string, FieldType> fieldTypes;
        std::unordered_map<std::string, bool> searchable;
        std::string vectorField;
        std::unordered_map<std::string, size_t> imageMaxKB;
        uint32_t schemaVersion = 1;
        std::string schemaId;
        struct DocIdConfig {
            std::string field;
            FieldType type = FieldType::Unknown;
            bool enforceUnique = true;
        };
        struct RelationConfig {
            std::string field;
            std::string targetIndex;
            bool allowCrossIndex = true;
        };
        std::optional<DocIdConfig> docId;
        std::optional<RelationConfig> relation;
    };

    explicit BlackBox(const std::string& dataDir = "");
    ~BlackBox();

    // Index management
    bool createIndex(const std::string& name, const IndexSchema& schema);
    bool indexExists(const std::string& name) const;
    const IndexSchema* getSchema(const std::string& name) const;
    const std::unordered_map<std::string, std::unordered_map<DocId, double>>* getNumericValues(const std::string& name) const;
    const std::unordered_map<std::string, std::unordered_map<DocId, bool>>* getBoolValues(const std::string& name) const;
    const std::unordered_map<std::string, std::unordered_map<std::string, std::vector<DocId>>>* getStringLists(const std::string& name) const;
    const std::unordered_map<DocId, double>* getNumericFieldCached(const std::string& name, const std::string& field) const;

    // Index a document from a JSON string and return its ID in the given index.
    DocId indexDocument(const std::string& index, const std::string& jsonStr);
    // Update a document (partial merge if partial=true, else replace).
    bool updateDocument(const std::string& index, DocId id, const std::string& jsonStr, bool partial);

    // Search with selectable algorithm: "lexical", "bm25", "bm25_or", "fuzzy", "semantic", "phrase"
    std::vector<SearchHit> search(const std::string& index, const std::string& query, const std::string& mode = "bm25", size_t maxResults = 10, int maxEditDistance = 1) const;

    // Hybrid search: blend bm25 + semantic + lexical with weights
    std::vector<SearchHit> searchHybrid(const std::string& index, const std::string& query, double wBm25, double wSemantic, double wLexical, size_t maxResults) const;

    // Vector search (cosine similarity)
    std::vector<SearchHit> searchVector(const std::string& index, const std::vector<float>& queryVec, size_t maxResults, uint32_t probesOverride = 0, uint32_t efOverride = 0) const;

    // Retrieve a single document.
    nlohmann::json getDocument(const std::string& index, DocId id) const;
    std::optional<DocId> lookupDocId(const std::string& index, const std::string& providedId) const;
    std::optional<std::string> externalIdForDoc(const std::string& index, DocId id) const;
    std::optional<std::string> getImageBase64(const std::string& index, DocId id, const std::string& field) const;

    // Delete a document; returns true if removed.
    bool deleteDocument(const std::string& index, DocId id);

    // Utility: number of docs stored in an index
    std::size_t documentCount(const std::string& index) const;

    // Snapshot persistence (.skd manifest + shards)
    bool saveSnapshot(const std::string& path = "") const;
    bool loadSnapshot(const std::string& path = "");
    // Compatibility migration: rewrite manifests/WAL headers to latest formats.
    nlohmann::json migrateCompatibility(const std::string& snapshotPath = "") const;

    struct IndexStats {
        std::string name;
        std::size_t documents = 0;
        std::size_t segments = 0;
        std::size_t vectors = 0;
        std::size_t annClusters = 0;
        uint64_t walBytes = 0;
        std::size_t pendingOps = 0;
        double avgDocLen = 0.0;
        bool walSchemaMismatch = false;
        bool walUpgraded = false;
        std::string schemaId;
        uint32_t schemaVersion = 0;
        uint64_t replayErrors = 0;
        uint64_t annRecallSamples = 0;
        uint64_t annRecallHits = 0;
        uint64_t termCacheHits = 0;
        uint64_t termCacheMisses = 0;
        uint64_t fieldCacheHits = 0;
        uint64_t fieldCacheMisses = 0;
    };

    std::vector<IndexStats> stats() const;
    nlohmann::json config() const;
    nlohmann::json shippingPlan() const;
    nlohmann::json clusterState() const;
    uint64_t replayErrors() const;
    uint64_t annRecallSamples() const;
    uint64_t annRecallHits() const;
    std::string dataDir() const { return dataDir_; }
    bool createOrUpdateCustomApi(const std::string& name, const nlohmann::json& spec);
    bool removeCustomApi(const std::string& name);
    std::optional<nlohmann::json> getCustomApi(const std::string& name) const;
    nlohmann::json listCustomApis() const;
    nlohmann::json runCustomApi(const std::string& name, const nlohmann::json& params) const;
    std::vector<DocId> scanStoredEquals(const std::string& index, const std::string& field, const nlohmann::json& value) const;
    bool shouldBackpressure(const std::string& index) const;

private:
    struct ImageBlob { std::string format; std::string data; };
    struct ProcessedDoc {
        nlohmann::json doc;
        std::unordered_map<std::string, ImageBlob> images;
    };

    class DocumentCache {
    public:
        DocumentCache(size_t cap = 256, uint64_t maxBytes = 0) : capacityEntries_(cap), maxBytes_(maxBytes) {}
        bool get(const std::string& key, nlohmann::json& out) const {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = map_.find(key);
            if (it == map_.end()) {
                misses_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            hits_.fetch_add(1, std::memory_order_relaxed);
            touch(it);
            out = it->second.doc;
            return true;
        }
        void put(const std::string& key, const nlohmann::json& doc) const {
            if (capacityEntries_ == 0) return;
            std::lock_guard<std::mutex> lk(mtx_);
            std::string dumped = doc.dump();
            size_t sz = dumped.size();
            auto it = map_.find(key);
            if (it != map_.end()) {
                totalBytes_ = totalBytes_ - it->second.bytes + sz;
                it->second.bytes = sz;
                it->second.doc = doc;
                touch(it);
                evictIfNeeded(0);
                return;
            }
            evictIfNeeded(sz);
            order_.push_front(key);
            map_[key] = {doc, sz, order_.begin()};
            totalBytes_ += sz;
        }
        void setCapacity(size_t cap) const { capacityEntries_ = cap; evictIfNeeded(0); }
        void setMaxBytes(uint64_t bytes) const { maxBytes_ = bytes; evictIfNeeded(0); }
        uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
        uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }
        uint64_t totalBytes() const { return totalBytes_; }
        void erase(const std::string& key) const {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = map_.find(key);
            if (it == map_.end()) return;
            if (totalBytes_ >= it->second.bytes) totalBytes_ -= it->second.bytes;
            order_.erase(it->second.it);
            map_.erase(it);
        }
    private:
        struct Entry {
            nlohmann::json doc;
            size_t bytes = 0;
            std::list<std::string>::iterator it;
        };
        mutable size_t capacityEntries_;
        mutable uint64_t maxBytes_;
        mutable uint64_t totalBytes_ = 0;
        mutable std::list<std::string> order_;
        mutable std::unordered_map<std::string, Entry> map_;
        mutable std::atomic<uint64_t> hits_{0};
        mutable std::atomic<uint64_t> misses_{0};
        mutable std::mutex mtx_;

        void evictIfNeeded(size_t incoming) const {
            while ((!order_.empty()) && (order_.size() >= capacityEntries_ || (maxBytes_ > 0 && totalBytes_ + incoming > maxBytes_))) {
                auto evict = order_.back();
                auto it = map_.find(evict);
                if (it != map_.end()) {
                    if (totalBytes_ >= it->second.bytes) totalBytes_ -= it->second.bytes;
                }
                order_.pop_back();
                map_.erase(evict);
            }
        }
        void touch(std::unordered_map<std::string, Entry>::iterator it) const {
            order_.erase(it->second.it);
            order_.push_front(it->first);
            it->second.it = order_.begin();
        }
    };
    class TermCache {
    public:
        TermCache(size_t cap = 1024, uint64_t maxBytes = 0) : capacityEntries_(cap), maxBytes_(maxBytes) {}
        bool get(const std::string& key, const void*& out) const {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = map_.find(key);
            if (it == map_.end()) {
                misses_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            hits_.fetch_add(1, std::memory_order_relaxed);
            touch(it);
            out = it->second.ptr;
            return true;
        }
        void put(const std::string& key, const void* ptr, size_t bytesEstimate) const {
            if (capacityEntries_ == 0 || ptr == nullptr) return;
            std::lock_guard<std::mutex> lk(mtx_);
            size_t sz = bytesEstimate;
            auto it = map_.find(key);
            if (it != map_.end()) {
                totalBytes_ = totalBytes_ - it->second.bytes + sz;
                it->second.bytes = sz;
                it->second.ptr = ptr;
                touch(it);
                evictIfNeeded(0);
                return;
            }
            evictIfNeeded(sz);
            order_.push_front(key);
            map_[key] = {ptr, sz, order_.begin()};
            totalBytes_ += sz;
        }
        void setCapacity(size_t cap) const { capacityEntries_ = cap; evictIfNeeded(0); }
        void setMaxBytes(uint64_t bytes) const { maxBytes_ = bytes; evictIfNeeded(0); }
        uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
        uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }
        uint64_t totalBytes() const { return totalBytes_; }
        void clear() const {
            std::lock_guard<std::mutex> lk(mtx_);
            order_.clear();
            map_.clear();
            totalBytes_ = 0;
        }
    private:
        struct Entry {
            const void* ptr;
            size_t bytes;
            std::list<std::string>::iterator it;
        };
        mutable size_t capacityEntries_;
        mutable uint64_t maxBytes_;
        mutable uint64_t totalBytes_ = 0;
        mutable std::list<std::string> order_;
        mutable std::unordered_map<std::string, Entry> map_;
        mutable std::atomic<uint64_t> hits_{0};
        mutable std::atomic<uint64_t> misses_{0};
        mutable std::mutex mtx_;
        void evictIfNeeded(size_t incoming) const {
            while ((!order_.empty()) && (order_.size() >= capacityEntries_ || (maxBytes_ > 0 && totalBytes_ + incoming > maxBytes_))) {
                auto evict = order_.back();
                auto it = map_.find(evict);
                if (it != map_.end()) {
                    if (totalBytes_ >= it->second.bytes) totalBytes_ -= it->second.bytes;
                }
                order_.pop_back();
                map_.erase(evict);
            }
        }
        void touch(std::unordered_map<std::string, Entry>::iterator it) const {
            order_.erase(it->second.it);
            order_.push_front(it->first);
            it->second.it = order_.begin();
        }
    };

    // Per-index state
    struct SegmentMetadata {
        std::string file;
        uint32_t minId = 0;
        uint32_t maxId = 0;
        uint64_t walPos = 0;
        std::vector<uint32_t> deletes;
    };

    struct IndexState {
        DocId nextId = 1;
        std::string name;
        std::unordered_map<DocId, nlohmann::json> documents;
        std::unordered_map<std::string, std::vector<algo::Posting>> invertedIndex;
        std::unordered_map<DocId, uint32_t> docLengths;
        double avgDocLen = 0.0;
        IndexSchema schema;
        std::unordered_map<DocId, std::vector<float>> vectors;
        // Doc-values for filters
        std::unordered_map<std::string, std::unordered_map<DocId, double>> numericValues;
        std::unordered_map<std::string, std::unordered_map<DocId, bool>> boolValues;
        std::unordered_map<std::string, std::unordered_map<std::string, std::vector<DocId>>> stringLists; // for array<string> fields
        std::unordered_map<std::string, std::unordered_map<DocId, ImageBlob>> imageValues;
        std::unordered_map<std::string, DocId> externalToDocId;
        std::unordered_map<DocId, std::string> docIdToExternal;
        std::unordered_map<std::string, std::vector<algo::SkipEntry>> skipPointers; // block-level skips
        // ANN coarse quantizer (lightweight IVF-like)
        bool annDirty = true;
        uint32_t annClusters = 8;
        std::vector<std::vector<float>> annCentroids;
        std::vector<std::vector<DocId>> annBuckets;
        std::unordered_map<DocId, std::vector<DocId>> annGraph;
        uint32_t annM = 16;
        uint32_t annEfSearch = 64;
        // query_values index
        std::unordered_map<std::string, std::vector<std::pair<DocId, double>>> queryValueIndex;
        std::unordered_map<DocId, std::vector<std::pair<std::string, double>>> queryValuesByDoc;
        std::vector<SegmentMetadata> segments;
        WalWriter wal;
        size_t opsSinceFlush = 0;
        uint64_t nextOpId = 1;
        bool manifestDirty = false;
        std::unordered_set<DocId> tombstones;
        std::unordered_set<DocId> persistedDeletes; // delete bitmap persisted in segment
        std::shared_ptr<std::mutex> mtx = std::make_shared<std::mutex>(); // per-index lock
        uint32_t annProbes = 2;
        uint64_t lastFlushedWalOffset = 0;
        std::chrono::steady_clock::time_point lastFlushAt = std::chrono::steady_clock::now();
        uint64_t replayErrors = 0;
        uint64_t annRecallSamples = 0;
        uint64_t annRecallHits = 0;
        uint64_t termCacheHits = 0;
        uint64_t termCacheMisses = 0;
        uint64_t fieldCacheHits = 0;
        uint64_t fieldCacheMisses = 0;
    };

    std::string dataDir_;
    std::string clusterId_;
    std::string shardId_;
    std::string replicaId_;
    std::string shipEndpoint_;
    std::string shipMethod_ = "http";
    mutable std::shared_mutex mutex_; // protects indexes_ map and custom APIs
    size_t flushEveryDocs_ = 5000;
    uint64_t flushEveryMs_ = 0;
    uint64_t flushWalBytesThreshold_ = 8 * 1024 * 1024;
    size_t mergeSegmentsAt_ = 10;
    uint64_t mergeThrottleMs_ = 0;
    uint64_t mergeMaxMB_ = 0;
    double mergeMBps_ = 0.0;
    uint64_t mergeIoBudgetBytes_ = 0;
    size_t snapshotCacheCapacity_ = 8;
    uint64_t snapshotCacheMaxBytes_ = 0; // 0 = unlimited
    size_t docCacheCapacity_ = 256;
    uint64_t docCacheMaxBytes_ = 0;
    size_t termCacheCapacity_ = 1024;
    uint64_t termCacheMaxBytes_ = 0;
    size_t fieldCacheCapacity_ = 512;
    uint64_t fieldCacheMaxBytes_ = 0;
    bool compressSnapshots_ = true;
    bool autoSnapshot_ = false;
    uint32_t defaultAnnClusters_ = 8;
    uint32_t defaultAnnProbes_ = 2;
    uint32_t defaultAnnM_ = 16;
    uint32_t defaultAnnEfSearch_ = 64;
    uint64_t walFlushBytes_ = 64 * 1024;
    uint64_t walFlushMs_ = 200;
    bool walFsyncEnabled_ = true;
    std::atomic<bool> manifestDirty_{false};
    std::thread maintenanceThread_;
    std::atomic<bool> stopMaintenance_{false};
    mutable std::unordered_map<std::string, IndexState> indexes_;
    std::unordered_map<std::string, nlohmann::json> customApis_;
    std::string customApiPath_;
    mutable std::atomic<uint64_t> replayErrors_{0};
    mutable std::atomic<uint64_t> annRecallSamples_{0};
    mutable std::atomic<uint64_t> annRecallHits_{0};
    mutable DocumentCache docCache_;
    mutable TermCache termCache_;
    mutable TermCache fieldCache_; // reused structure for doc-values maps
    std::deque<std::string> mergeQueue_;
    std::unordered_set<std::string> mergePending_;
    mutable std::mutex mergeMtx_;
    mutable std::mutex mergeIoMtx_;
    mutable std::condition_variable mergeCv_;
    uint64_t mergeOutstandingBytes_ = 0;

    void refreshAverages(IndexState& idx);
    uint64_t estimateMergeBytes(const IndexState& idx) const;

    // Tokenizer used for index + query
    std::vector<std::string> tokenize(const std::string& text) const;

    // Recursively walk JSON and index string fields
    void indexJson(IndexState& idx, DocId id, const nlohmann::json& j);
    void indexJsonRecursive(IndexState& idx, DocId id, const nlohmann::json& node);
    void indexStructured(IndexState& idx, DocId id, const nlohmann::json& doc);
    void indexQueryValues(IndexState& idx, DocId id, const std::string& field, const nlohmann::json& node);
    void clearQueryValues(IndexState& idx, DocId id);
    void rebuildQueryValues(IndexState& idx);

    // Remove indexed terms for a document
    void removeJson(IndexState& idx, DocId id, const nlohmann::json& j);
    void removeJsonRecursive(IndexState& idx, DocId id, const nlohmann::json& node);

    // Posting helpers
    void addPosting(IndexState& idx, const std::string& term, DocId id, uint32_t tf);
    void removePosting(IndexState& idx, const std::string& term, DocId id);
    void rebuildSkipPointers(IndexState& idx);
    void rebuildAnn(IndexState& idx) const;
    void rebuildHnsw(IndexState& idx) const;
    std::vector<SearchHit> searchHnsw(IndexState& idx, const std::vector<float>& queryVec, size_t maxResults) const;
    void applyQueryValueBoost(const std::string& query, const IndexState& idx, size_t maxResults, std::vector<SearchHit>& hits) const;

    bool validateDocument(const IndexState& idx, const nlohmann::json& doc) const;

    // Snapshot helpers are implemented in the cpp.
    DocId applyUpsert(IndexState& idx, DocId id, const ProcessedDoc& processed, bool logWal);
    bool applyDelete(IndexState& idx, DocId id, bool logWal);
    void flushIfNeeded(const std::string& index, IndexState& idx);
    void maybeMergeSegments(const std::string& index, IndexState& idx);
    void enqueueMerge(const std::string& index);
    void processMergeQueue();
    bool performMerge(const std::string& index, IndexState& idx);
    bool writeManifest() const;
    void replayWal(IndexState& idx, uint64_t startOffset = 0);
    void loadWalOnly();
    void configureSchema(IndexState& state);
    void persistSchema(const std::string& name, const IndexState& state) const;
    std::optional<std::string> extractCustomId(const IndexState& idx, const nlohmann::json& doc) const;
    std::optional<std::string> canonicalizeCustomIdInput(const IndexState& idx, const std::string& raw) const;
    std::optional<DocId> findDocIdUnlocked(const IndexState& idx, const std::string& providedId) const;
    ProcessedDoc preprocessIncomingDocument(IndexState& idx, const nlohmann::json& doc) const;
    ProcessedDoc preprocessWalDocument(IndexState& idx, nlohmann::json& walDoc) const;
    void attachImagesToWal(json& walDoc, const ProcessedDoc& processed) const;
    void loadCustomApis();
    void saveCustomApis() const;
    bool validateCustomApi(const std::string& name, const nlohmann::json& spec) const;
    nlohmann::json executeCustomApiInternal(const std::string& name, const nlohmann::json& spec, const nlohmann::json& params) const;
    nlohmann::json buildCustomRelationTree(const std::string& baseIndex, const nlohmann::json& doc, DocId docId, const nlohmann::json& relationSpec) const;
    void startMaintenance();
    void stopMaintenance();
};

} // namespace minielastic
