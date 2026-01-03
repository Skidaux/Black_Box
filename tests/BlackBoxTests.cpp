#include "BlackBox.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <thread>

using minielastic::BlackBox;
using nlohmann::json;

static void expect(bool cond, const std::string& msg) {
    if (!cond) {
        std::cerr << "Test failed: " << msg << std::endl;
        std::exit(1);
    }
}

int main() {
    const std::string dataDir = "testdata";
    std::filesystem::remove_all(dataDir);
    BlackBox db(dataDir);
    const std::string indexName = "test_index";
    BlackBox::IndexSchema schema;
    schema.schema = {
        {"fields", {
            {"title", "text"},
            {"body", "text"}
        }}
    };
    expect(db.createIndex(indexName, schema), "failed to create index");

    // Index a doc
    const std::string doc = R"({"title":"Doc1","body":"quick brown fox"})";
    auto id = db.indexDocument(indexName, doc);
    expect(id == 1, "expected id 1");
    expect(db.documentCount(indexName) == 1, "doc count should be 1");

    // Default (bm25 with lexical fallback)
    auto bm25 = db.search(indexName, "quick fox");
    expect(!bm25.empty() && bm25[0].id == id, "bm25 should find doc");

    // Lexical
    auto lex = db.search(indexName, "quick fox", "lexical");
    expect(!lex.empty() && lex[0].id == id, "lexical should find doc");

    // Fuzzy
    auto fuzzy = db.search(indexName, "quik", "fuzzy", 10, 2);
    expect(!fuzzy.empty() && fuzzy[0].id == id, "fuzzy should find doc");

    // Semantic / tfidf
    auto sem = db.search(indexName, "quick brown fox", "semantic");
    expect(!sem.empty() && sem[0].id == id, "semantic should find doc");

    // Snapshot save/load roundtrip
    // Persist and reload
    expect(db.saveSnapshot(), "snapshot save");

    BlackBox db2(dataDir);
    auto bm25b = db2.search(indexName, "quick fox");
    expect(!bm25b.empty() && bm25b[0].id == id, "bm25 after load should find doc");

    // Custom doc IDs + relations + image field
    const std::string relIndex = "custom_index";
    BlackBox::IndexSchema relSchema;
    relSchema.schema = {
        {"fields", {
            {"title", "text"},
            {"body", "text"},
            {"sku", "text"},
            {"favicon", {
                {"type", "image"},
                {"max_kb", 4}
            }}
        }},
        {"doc_id", {
            {"field", "sku"},
            {"type", "string"}
        }},
        {"relation", {
            {"field", "parent"},
            {"target_index", ""}
        }}
    };
    expect(db.createIndex(relIndex, relSchema), "failed to create relation index");

    json imagePayload = {
        {"format", "png"},
        {"encoding", "base64"},
        {"content", "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mO0dOjYfwAJ7APc6+UeFwAAAABJRU5ErkJggg=="}
    };

    auto parentId = db.indexDocument(relIndex, json({
        {"sku","sku-1"},
        {"title","Parent"},
        {"body","root"},
        {"favicon", imagePayload}
    }).dump());
    auto childId = db.indexDocument(relIndex, json({
        {"sku","sku-2"},
        {"title","Child"},
        {"body","leaf"},
        {"parent",{ {"id","sku-1"} }},
        {"favicon", imagePayload}
    }).dump());
    expect(childId != parentId, "child should be distinct doc id");
    auto parentLookup = db.lookupDocId(relIndex, "sku-1");
    expect(parentLookup.has_value() && *parentLookup == parentId, "lookup should return parent id");
    auto childLookup = db.lookupDocId(relIndex, "sku-2");
    expect(childLookup.has_value() && *childLookup == childId, "lookup should return child id");
    auto parentDoc = db.getDocument(relIndex, *parentLookup);
    expect(parentDoc["sku"].get<std::string>() == "sku-1", "stored parent doc mismatch");
    expect(parentDoc.contains("favicon"), "favicon metadata missing");
    expect(db.deleteDocument(relIndex, *childLookup), "delete child");
    expect(!db.lookupDocId(relIndex, "sku-2").has_value(), "child lookup should be cleared");

    // Delete durability: index + delete + restart should not resurrect
    const std::string delIndex = "del_index";
    BlackBox::IndexSchema delSchema;
    delSchema.schema = {
        {"fields", {
            {"title", "text"},
            {"body", "text"}
        }}
    };
    expect(db.createIndex(delIndex, delSchema), "failed to create delete index");
    auto did = db.indexDocument(delIndex, R"({"title":"T","body":"to be removed"})");
    expect(db.deleteDocument(delIndex, did), "delete doc before snapshot");
    expect(db.documentCount(delIndex) == 0, "doc count should be 0 after delete");
    expect(db.saveSnapshot(), "snapshot save after delete");
    {
        BlackBox dbReload(dataDir);
        auto hits = dbReload.search(delIndex, "removed");
        expect(hits.empty(), "deleted doc should not resurrect after reload");
        expect(dbReload.documentCount(delIndex) == 0, "doc count should remain 0 after reload");
    }

    // Update path should refresh _updated_at and preserve id
    auto newId = db.indexDocument(indexName, R"({"title":"Doc2","body":"foo bar"})");
    auto before = db.getDocument(indexName, newId)["_updated_at"].get<int64_t>();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    expect(db.updateDocument(indexName, newId, R"({"body":"foo baz"})", true), "partial update");
    auto after = db.getDocument(indexName, newId)["_updated_at"].get<int64_t>();
    expect(after > before, "_updated_at should advance on update");

    // Bulk indexing via HTTP client simulated: ensure uniqueness enforcement on doc_id
    const std::string uniqIndex = "uniq_index";
    BlackBox::IndexSchema uniqSchema;
    uniqSchema.schema = {
        {"fields", {{"title", "text"}, {"sku", "text"}}},
        {"doc_id", {{"field", "sku"}, {"type", "string"}, {"enforce_unique", true}}}
    };
    expect(db.createIndex(uniqIndex, uniqSchema), "create uniq index");
    auto uid1 = db.indexDocument(uniqIndex, R"({"title":"first","sku":"ABC"})");
    expect(uid1 == 1, "first doc id");
    bool threw = false;
    try {
        db.indexDocument(uniqIndex, R"({"title":"dupe","sku":"ABC"})");
    } catch (...) {
        threw = true;
    }
    expect(threw, "duplicate doc_id should throw");

    // Fuzzy search returns results with edit distance
    auto fuzzy2 = db.search(indexName, "quik", "fuzzy", 10, 2);
    expect(!fuzzy2.empty(), "fuzzy search should still return");

    std::cout << "All tests passed." << std::endl;
    return 0;
}
