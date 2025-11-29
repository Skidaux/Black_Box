#include "BlackBox.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

using minielastic::BlackBox;

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

    // Index a doc
    const std::string doc = R"({"title":"Doc1","body":"quick brown fox"})";
    auto id = db.indexDocument(doc);
    expect(id == 1, "expected id 1");
    expect(db.documentCount() == 1, "doc count should be 1");

    // Default (bm25 with lexical fallback)
    auto bm25 = db.search("quick fox");
    expect(!bm25.empty() && bm25[0].id == id, "bm25 should find doc");

    // Lexical
    auto lex = db.search("quick fox", "lexical");
    expect(!lex.empty() && lex[0].id == id, "lexical should find doc");

    // Fuzzy
    auto fuzzy = db.search("quik", "fuzzy", 10, 2);
    expect(!fuzzy.empty() && fuzzy[0].id == id, "fuzzy should find doc");

    // Semantic / tfidf
    auto sem = db.search("quick brown fox", "semantic");
    expect(!sem.empty() && sem[0].id == id, "semantic should find doc");

    // Snapshot save/load roundtrip
    const auto snapPath = std::filesystem::path(dataDir) / "index.skd";
    expect(db.saveSnapshot(snapPath.string()), "snapshot save");

    BlackBox db2(dataDir);
    auto bm25b = db2.search("quick fox");
    expect(!bm25b.empty() && bm25b[0].id == id, "bm25 after load should find doc");

    std::cout << "All tests passed." << std::endl;
    return 0;
}
