//BlackBox.hpp
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include "minielastic/Analyzer.hpp"
#include "minielastic/algorithms/SearchAlgorithms.hpp"

namespace minielastic {

class BlackBox {
public:
    using DocId = uint32_t;
    using SearchHit = algo::SearchHit;

    struct IndexSchema {
        nlohmann::json schema;
    };

    explicit BlackBox(const std::string& dataDir = "");
    ~BlackBox();

    // Index management
    bool createIndex(const std::string& name, const IndexSchema& schema);
    bool indexExists(const std::string& name) const;

    // Index a document from a JSON string and return its ID in the given index.
    DocId indexDocument(const std::string& index, const std::string& jsonStr);

    // Search with selectable algorithm: "lexical", "bm25", "fuzzy", "semantic"
    std::vector<SearchHit> search(const std::string& index, const std::string& query, const std::string& mode = "bm25", size_t maxResults = 10, int maxEditDistance = 1) const;

    // Hybrid search: blend bm25 + semantic + lexical with weights
    std::vector<SearchHit> searchHybrid(const std::string& index, const std::string& query, double wBm25, double wSemantic, double wLexical, size_t maxResults) const;

    // Retrieve a single document.
    nlohmann::json getDocument(const std::string& index, DocId id) const;

    // Delete a document; returns true if removed.
    bool deleteDocument(const std::string& index, DocId id);

    // Utility: number of docs stored in an index
    std::size_t documentCount(const std::string& index) const;

    // Snapshot persistence (.skd manifest + shards)
    bool saveSnapshot(const std::string& path = "") const;
    bool loadSnapshot(const std::string& path = "");

private:
    // Per-index state
    struct IndexState {
        DocId nextId = 1;
        std::unordered_map<DocId, nlohmann::json> documents;
        std::unordered_map<std::string, std::vector<algo::Posting>> invertedIndex;
        std::unordered_map<DocId, uint32_t> docLengths;
        double avgDocLen = 0.0;
        IndexSchema schema;
    };

    std::string dataDir_;
    std::unordered_map<std::string, IndexState> indexes_;

    void refreshAverages(IndexState& idx);

    // Tokenizer used for index + query
    std::vector<std::string> tokenize(const std::string& text) const;

    // Recursively walk JSON and index string fields
    void indexJson(IndexState& idx, DocId id, const nlohmann::json& j);
    void indexJsonRecursive(IndexState& idx, DocId id, const nlohmann::json& node);

    // Remove indexed terms for a document
    void removeJson(IndexState& idx, DocId id, const nlohmann::json& j);
    void removeJsonRecursive(IndexState& idx, DocId id, const nlohmann::json& node);

    // Posting helpers
    void addPosting(IndexState& idx, const std::string& term, DocId id, uint32_t tf);
    void removePosting(IndexState& idx, const std::string& term, DocId id);

    // Snapshot helpers are implemented in the cpp.
};

} // namespace minielastic
