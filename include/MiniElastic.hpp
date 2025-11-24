//MiniElastic.hpp
#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace minielastic {

class MiniElastic {
public:
    using DocId = uint32_t;

    MiniElastic() = default;

    // --- Public API ---

    // Index a document from a JSON string and return its ID.
    DocId indexDocument(const std::string& jsonStr);

    // Search for terms (simple AND semantics).
    std::vector<DocId> search(const std::string& query) const;

    // Retrieve a single document.
    nlohmann::json getDocument(DocId id) const;

    // Utility: number of docs stored
    std::size_t documentCount() const { return documents_.size(); }

private:
    // --- Internal state ---
    DocId nextId_ = 1;

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

    // A helper to add docId to term posting lists
    void addPosting(const std::string& term, DocId id);
};

} // namespace minielastic
