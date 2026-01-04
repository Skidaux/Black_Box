#include "BlackBox.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
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
try {
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

    // Non-searchable and query_values fields
    const std::string extraIndex = "meta_index";
    BlackBox::IndexSchema extraSchema;
    extraSchema.schema = {
        {"fields", {
            {"title", "text"},
            {"version", {{"type","number"}, {"searchable", false}}},
            {"queries", {{"type","query_values"}, {"searchable", true}}}
        }}
    };
    expect(db.createIndex(extraIndex, extraSchema), "create meta index");
    auto mid = db.indexDocument(extraIndex, R"({"title":"Versioned","version":1,"queries":[{"query":"yt","score":1.0}]})");
    // version should be stored but not searchable; query_values should boost
    auto hitsLex = db.search(extraIndex, "version", "bm25");
    expect(hitsLex.empty(), "non-searchable field should not be indexed");
    auto hitsBoost = db.search(extraIndex, "yt", "bm25");
    expect(!hitsBoost.empty() && hitsBoost[0].id == mid, "query_values boost should surface doc");
    auto storedMatch = db.scanStoredEquals(extraIndex, "version", 1);
    expect(storedMatch.size() == 1 && storedMatch[0] == mid, "stored-match should find non-searchable version");

    // WAL header should be versioned and carry magic
    {
        std::ifstream wal((std::filesystem::path(dataDir) / (extraIndex + ".wal")).string(), std::ios::binary);
        expect(static_cast<bool>(wal), "wal should exist");
        char magic[5] = {0};
        wal.read(magic, 5);
        expect(std::string(magic, 5) == "BBWAL", "wal magic should be BBWAL");
    }

    // Manifest should include format/version/schema_id/next_op_id
    expect(db.saveSnapshot(), "snapshot save for manifest inspection");
    {
        std::ifstream manifestIn(std::filesystem::path(dataDir) / "index.manifest");
        expect(static_cast<bool>(manifestIn), "manifest readable");
        json manifest = json::parse(manifestIn, nullptr, false);
        expect(!manifest.is_discarded(), "manifest parse");
        expect(manifest.value("format", "") == "blackbox_manifest", "manifest format tag");
        expect(manifest.value("version", 0) >= 2, "manifest version >=2");
        bool found = false;
        for (const auto& idx : manifest.value("indexes", json::array())) {
            if (!idx.is_object()) continue;
            if (idx.value("name", "") != extraIndex) continue;
            found = true;
            auto schemaId = idx.value("schema_id", std::string());
            expect(!schemaId.empty(), "schema_id present in manifest");
            auto nextOp = idx.value("next_op_id", 0ull);
            expect(nextOp > 0, "next_op_id populated");
            break;
        }
        expect(found, "extra index present in manifest");
    }

    // Reload and ensure query_values + stored-match survive
    {
        BlackBox dbReload(dataDir);
        auto hitsReload = dbReload.search(extraIndex, "yt", "bm25");
        expect(!hitsReload.empty() && hitsReload[0].id == mid, "reload query_values boost");
        auto storedReload = dbReload.scanStoredEquals(extraIndex, "version", 1);
        expect(storedReload.size() == 1 && storedReload[0] == mid, "reload stored-match non-searchable");
    }

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

    // Non-searchable array field membership scan
    const std::string arrayIndex = "array_meta";
    BlackBox::IndexSchema arraySchema;
    arraySchema.schema = {
        {"fields", {
            {"tags", {{"type","array"}, {"searchable", false}}},
            {"title", "text"}
        }}
    };
    expect(db.createIndex(arrayIndex, arraySchema), "create array index");
    auto aid = db.indexDocument(arrayIndex, R"({"title":"hidden tags","tags":["foo","bar"]})");
    auto tagSearch = db.search(arrayIndex, "foo", "bm25");
    expect(tagSearch.empty(), "non-searchable array should not be indexed");
    auto tagStored = db.scanStoredEquals(arrayIndex, "tags", "foo");
    expect(tagStored.size() == 1 && tagStored[0] == aid, "stored-match should find array member");

    // Query-values stored-only should not boost
    const std::string qvStoredIndex = "qv_stored";
    BlackBox::IndexSchema qvStoredSchema;
    qvStoredSchema.schema = {
        {"fields", {
            {"title", "text"},
            {"queries", {{"type","query_values"}, {"searchable", false}}}
        }}
    };
    expect(db.createIndex(qvStoredIndex, qvStoredSchema), "create qv stored-only index");
    auto qvsId = db.indexDocument(qvStoredIndex, R"({"title":"plain","queries":[{"query":"boostme","score":1.0}]})");
    auto qvsHits = db.search(qvStoredIndex, "boostme", "bm25");
    expect(qvsHits.empty(), "query_values stored-only should not affect search");
    auto qvsStored = db.scanStoredEquals(qvStoredIndex, "queries", json::object({{"query","boostme"},{"score",1.0}}));
    expect(qvsStored.size() == 1 && qvsStored[0] == qvsId, "stored-match should find query_values payload");

    // Doc_id uniqueness: updating same doc_id should be allowed
    auto uid1b = db.lookupDocId(uniqIndex, "ABC");
    expect(uid1b.has_value(), "doc_id should be present");
    expect(db.updateDocument(uniqIndex, *uid1b, R"({"title":"updated title"})", true), "update existing doc_id");
    auto updatedDoc = db.getDocument(uniqIndex, *uid1b);
    expect(updatedDoc["title"] == "updated title", "doc updated in place");

    // Relation validation when cross-index not allowed
    const std::string relStrictIdx = "rel_strict";
    BlackBox::IndexSchema relStrictSchema;
    relStrictSchema.schema = {
        {"fields", {{"title", "text"}, {"parent", "text"}}},
        {"relation", {{"field","parent"}, {"target_index","rel_strict"}, {"allow_cross_index", false}}}
    };
    expect(db.createIndex(relStrictIdx, relStrictSchema), "create strict relation index");
    bool relThrew = false;
    try {
        db.indexDocument(relStrictIdx, R"({"title":"bad","parent":{"id":"1","index":"other"}})");
    } catch (...) {
        relThrew = true;
    }
    expect(relThrew, "cross-index relation should be rejected when disallowed");

    // Persist current state so reopened instances see newly added indexes
    expect(db.saveSnapshot(), "snapshot save before wal reopen");

    // WAL header persistence across reopen (ensure magic is present after new header write)
    {
        BlackBox dbWal(dataDir);
        auto wid = dbWal.indexDocument(arrayIndex, R"({"title":"wal-ping","tags":["wal"]})");
        expect(wid > 0, "wal write doc");
    }
    {
        std::ifstream wal((std::filesystem::path(dataDir) / (arrayIndex + ".wal")).string(), std::ios::binary);
        expect(static_cast<bool>(wal), "wal exists after reopen");
        char magic[5] = {0};
        wal.read(magic, 5);
        expect(std::string(magic, 5) == "BBWAL", "wal magic present after reopen");
    }

    // Fuzzy search returns results with edit distance
    auto fuzzy2 = db.search(indexName, "quik", "fuzzy", 10, 2);
    expect(!fuzzy2.empty(), "fuzzy search should still return");

    // Stats should surface WAL mismatch/upgrade flags (default false)
    auto stats = db.stats();
    expect(!stats.empty(), "stats not empty");
    for (const auto& st : stats) {
        expect(!st.walSchemaMismatch, "wal schema mismatch should be false");
        expect(!st.walUpgraded, "wal upgraded should be false");
    }

    std::cout << "All tests passed." << std::endl;
    return 0;
} catch (const std::exception& e) {
    std::cerr << "Test failed with exception: " << e.what() << std::endl;
    return 1;
} catch (...) {
    std::cerr << "Test failed with unknown exception" << std::endl;
    return 1;
}
}
