//MiniElastic.cpp
#include "MiniElastic.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>

using json = nlohmann::json;

namespace minielastic {

// -----------------------------------------------------------
// CTOR/DTOR
// -----------------------------------------------------------
MiniElastic::MiniElastic(const std::string& dataDir) {
    if (!dataDir.empty()) {
        logStore_ = std::make_unique<LogStore>(dataDir);
        persistenceEnabled_ = logStore_->good();
        if (persistenceEnabled_) {
            loadFromLog();
        }
    }
}

MiniElastic::~MiniElastic() {
}

// -----------------------------------------------------------
// PUBLIC: Index a document
// -----------------------------------------------------------
MiniElastic::DocId MiniElastic::indexDocument(const std::string& jsonStr) {
    json j = json::parse(jsonStr); // may throw; callers should catch

    DocId id = nextId_++;
    putDocumentInternal(id, j);

    if (persistenceEnabled_) {
        logStore_->append(LogRecord{LogRecord::Op::Put, id, j});
    }

    return id;
}

// -----------------------------------------------------------
// PUBLIC: Get a document by ID
// -----------------------------------------------------------
json MiniElastic::getDocument(DocId id) const {
    auto it = documents_.find(id);
    if (it == documents_.end()) {
        throw std::runtime_error("Document ID not found");
    }
    return it->second;
}

// -----------------------------------------------------------
// PUBLIC: Delete a document
// -----------------------------------------------------------
bool MiniElastic::deleteDocument(DocId id) {
    return deleteDocumentInternal(id, true);
}

bool MiniElastic::deleteDocumentInternal(DocId id, bool persist) {
    auto it = documents_.find(id);
    if (it == documents_.end()) {
        return false;
    }

    // Remove indexed terms before dropping the document
    removeJson(id, it->second);
    documents_.erase(it);

    if (persist && persistenceEnabled_) {
        logStore_->append(LogRecord{LogRecord::Op::Del, id, {}});
    }

    return true;
}

// -----------------------------------------------------------
// PUBLIC: Search for documents (AND semantics)
// -----------------------------------------------------------
std::vector<MiniElastic::DocId> MiniElastic::search(const std::string& query) const {
    auto terms = tokenize(query);
    if (terms.empty()) return {};

    // Build list of posting lists
    std::vector<std::vector<DocId>> postingLists;
    for (const auto& term : terms) {
        auto it = invertedIndex_.find(term);
        if (it == invertedIndex_.end()) {
            return {}; // no documents contain this term
        }
        postingLists.push_back(it->second);
    }

    // Sort & unique first posting list
    std::vector<DocId> result = postingLists[0];
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    // Intersect with others
    for (size_t i = 1; i < postingLists.size(); ++i) {
        auto& pl = postingLists[i];

        std::sort(pl.begin(), pl.end());
        pl.erase(std::unique(pl.begin(), pl.end()), pl.end());

        std::vector<DocId> intersection;
        std::set_intersection(
            result.begin(), result.end(),
            pl.begin(), pl.end(),
            std::back_inserter(intersection)
        );
        result.swap(intersection);

        if (result.empty()) break;
    }

    return result;
}

// -----------------------------------------------------------
// PRIVATE: Tokenizer
// -----------------------------------------------------------
std::vector<std::string> MiniElastic::tokenize(const std::string& text) const {
    return Analyzer::tokenize(text);
}

// -----------------------------------------------------------
// PRIVATE: Index JSON document
// -----------------------------------------------------------
void MiniElastic::indexJson(DocId id, const json& j) {
    indexJsonRecursive(id, j);
}

// Recursively walk through objects/arrays/strings
void MiniElastic::indexJsonRecursive(DocId id, const json& node) {
    if (node.is_string()) {
        // Index the string value
        auto terms = tokenize(node.get<std::string>());
        for (const auto& term : terms) {
            addPosting(term, id);
        }
    }
    else if (node.is_array()) {
        for (const auto& element : node) {
            indexJsonRecursive(id, element);
        }
    }
    else if (node.is_object()) {
        for (const auto& it : node.items()) {
            // If you want to index field names, do it here:
            // addPosting(it.key(), id);
            indexJsonRecursive(id, it.value());
        }
    }
    // Numbers / bool / null ignored for now
}

// -----------------------------------------------------------
// PRIVATE: Remove all postings for a document
// -----------------------------------------------------------
void MiniElastic::removeJson(DocId id, const json& j) {
    removeJsonRecursive(id, j);
}

void MiniElastic::removeJsonRecursive(DocId id, const json& node) {
    if (node.is_string()) {
        auto terms = Analyzer::tokenize(node.get<std::string>());
        for (const auto& term : terms) {
            removePosting(term, id);
        }
    }
    else if (node.is_array()) {
        for (const auto& element : node) {
            removeJsonRecursive(id, element);
        }
    }
    else if (node.is_object()) {
        for (const auto& it : node.items()) {
            removeJsonRecursive(id, it.value());
        }
    }
}

// -----------------------------------------------------------
// PRIVATE: Add posting with no duplicates inside the vector
// -----------------------------------------------------------
void MiniElastic::addPosting(const std::string& term, DocId id) {
    auto& vec = invertedIndex_[term];

    // Avoid duplicates (simple)
    if (vec.empty() || vec.back() != id) {
        vec.push_back(id);
    }
}

void MiniElastic::removePosting(const std::string& term, DocId id) {
    auto it = invertedIndex_.find(term);
    if (it == invertedIndex_.end()) return;

    auto& vec = it->second;
    vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());

    if (vec.empty()) {
        invertedIndex_.erase(it);
    }
}

// -----------------------------------------------------------
// PRIVATE: Persistence helpers
// -----------------------------------------------------------
void MiniElastic::loadFromLog() {
    if (!logStore_) return;
    logStore_->load([this](const LogRecord& rec) {
        if (rec.op == LogRecord::Op::Put) {
            putDocumentInternal(rec.id, rec.doc);
            nextId_ = std::max<DocId>(nextId_, rec.id + 1);
        } else if (rec.op == LogRecord::Op::Del) {
            deleteDocumentInternal(rec.id, false);
        }
    });
}

void MiniElastic::putDocumentInternal(DocId id, const json& doc) {
    documents_[id] = doc;
    indexJson(id, doc);
}

} // namespace minielastic
