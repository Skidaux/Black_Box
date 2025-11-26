//MiniElastic.hpp
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include "minielastic/LogStore.hpp"
#include "minielastic/Analyzer.hpp"

namespace minielastic {

class MiniElastic {
public:
    using DocId = uint32_t;

    explicit MiniElastic(const std::string& dataDir = "");
    ~MiniElastic();

    // --- Public API ---

    // Index a document from a JSON string and return its ID.
    DocId indexDocument(const std::string& jsonStr);

    // Search for terms (simple AND semantics).
    std::vector<DocId> search(const std::string& query) const;

    // Retrieve a single document.
    nlohmann::json getDocument(DocId id) const;

    // Delete a document; returns true if removed.
    bool deleteDocument(DocId id);

    // Utility: number of docs stored
    std::size_t documentCount() const { return documents_.size(); }

    bool persistenceEnabled() const { return persistenceEnabled_; }

private:
    // --- Internal state ---
    DocId nextId_ = 1;
    bool persistenceEnabled_ = false;
    std::unique_ptr<LogStore> logStore_;

    // Forward index: docId -> document JSON
    std::unordered_map<DocId, nlohmann::json> documents_;

    // Inverted index: term -> list of document IDs
    std::unordered_map<std::string, std::vector<DocId>> invertedIndex_;

    // --- Internal helpers ---

    // Tokenizer used for index + query
    std::vector<std::string> tokenize(const std::string& text) const;

    // Recursively walk JSON and index string fields
    void indexJson(DocId id, const nlohmann::json& j);
    void indexJsonRecursive(DocId id, const nlohmann::json& node);

    // Remove indexed terms for a document
    void removeJson(DocId id, const nlohmann::json& j);
    void removeJsonRecursive(DocId id, const nlohmann::json& node);

    // A helper to add docId to term posting lists
    void addPosting(const std::string& term, DocId id);
    void removePosting(const std::string& term, DocId id);

    // --- Persistence helpers ---
    void loadFromLog();

    // Internal helpers to avoid double persistence during replay
    void putDocumentInternal(DocId id, const nlohmann::json& doc);
    bool deleteDocumentInternal(DocId id, bool persist);
};

} // namespace minielastic
