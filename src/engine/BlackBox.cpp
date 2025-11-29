//BlackBox.cpp
#include "BlackBox.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string_view>
#include "minielastic/Checksum.hpp"

using json = nlohmann::json;

namespace {

template <typename T>
void writeLE(std::string& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<char>((static_cast<uint64_t>(value) >> (8 * i)) & 0xFFu));
    }
}

template <typename T>
bool readLE(std::string_view data, size_t& offset, T& value) {
    if (offset + sizeof(T) > data.size()) return false;
    uint64_t v = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        v |= static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (8 * i);
    }
    value = static_cast<T>(v);
    offset += sizeof(T);
    return true;
}

struct SnapshotChunk {
    std::vector<std::pair<uint32_t, json>> docs;
    std::unordered_map<uint32_t, uint32_t> docLens;
    std::unordered_map<std::string, std::vector<minielastic::algo::Posting>> index;
};

static bool writeSnapshotFile(const std::string& path,
                              const SnapshotChunk& chunk,
                              uint32_t nextId,
                              double avgDocLen) {
    struct Section {
        uint16_t id;
        uint16_t encoding;
        uint64_t offset = 0;
        std::string payload;
        uint32_t crc = 0;
    };

    auto makeSection = [](uint16_t id, std::string payload) {
        Section s{id, 0, 0, std::move(payload), 0};
        s.crc = minielastic::crc32(s.payload);
        return s;
    };

    json meta = {
        {"version", 1},
        {"doc_count", chunk.docs.size()},
        {"next_id", nextId},
        {"avg_doc_len", avgDocLen}
    };
    Section metaSec = makeSection(1, meta.dump());

    std::vector<std::pair<uint32_t, json>> docs(chunk.docs.begin(), chunk.docs.end());
    std::sort(docs.begin(), docs.end(), [](auto& a, auto& b) { return a.first < b.first; });

    std::string docTable;
    std::string docBlob;
    constexpr uint16_t kDocFlags = 0;
    for (const auto& pair : docs) {
        const uint32_t id = pair.first;
        auto serializedVec = json::to_cbor(pair.second);
        std::string serialized(serializedVec.begin(), serializedVec.end());
        uint64_t offset = docBlob.size();
        uint32_t len = static_cast<uint32_t>(serialized.size());
        uint32_t checksum = minielastic::crc32(serialized);

        writeLE(docTable, id);
        writeLE(docTable, kDocFlags);
        writeLE(docTable, offset);
        writeLE(docTable, len);
        writeLE(docTable, checksum);

        docBlob.append(serialized);
    }
    Section docTableSec = makeSection(3, std::move(docTable));
    Section docBlobSec = makeSection(4, std::move(docBlob));

    std::string docLensPayload;
    for (const auto& kv : chunk.docLens) {
        writeLE(docLensPayload, kv.first);
        writeLE(docLensPayload, static_cast<uint32_t>(kv.second));
    }
    Section docLenSec = makeSection(5, std::move(docLensPayload));

    std::vector<std::pair<std::string, std::vector<minielastic::algo::Posting>>> terms(chunk.index.begin(), chunk.index.end());
    std::sort(terms.begin(), terms.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    std::string termDict;
    std::string postings;

    for (const auto& termEntry : terms) {
        const auto& term = termEntry.first;
        if (term.size() > std::numeric_limits<uint16_t>::max()) continue;

        std::vector<minielastic::algo::Posting> postingList = termEntry.second;
        std::sort(postingList.begin(), postingList.end(), [](const minielastic::algo::Posting& a, const minielastic::algo::Posting& b) { return a.id < b.id; });
        postingList.erase(std::unique(postingList.begin(), postingList.end(), [](const minielastic::algo::Posting& a, const minielastic::algo::Posting& b){return a.id==b.id;}), postingList.end());

        std::string postingBytes;
        for (const auto& p : postingList) {
            writeLE(postingBytes, p.id);
            writeLE(postingBytes, static_cast<uint32_t>(p.tf));
        }

        uint64_t postingsOffset = postings.size();
        postings.append(postingBytes);
        uint32_t postingsLen = static_cast<uint32_t>(postingBytes.size());
        uint32_t postingsCrc = minielastic::crc32(postingBytes);

        writeLE(termDict, static_cast<uint16_t>(term.size()));
        termDict.append(term);
        writeLE(termDict, postingsOffset);
        writeLE(termDict, postingsLen);
        writeLE(termDict, static_cast<uint32_t>(postingList.size()));
        writeLE(termDict, postingsCrc);
    }

    Section termDictSec = makeSection(6, std::move(termDict));
    Section postingsSec = makeSection(7, std::move(postings));

    std::vector<Section> sections;
    sections.push_back(std::move(metaSec));
    sections.push_back(std::move(docTableSec));
    sections.push_back(std::move(docBlobSec));
    sections.push_back(std::move(docLenSec));
    sections.push_back(std::move(termDictSec));
    sections.push_back(std::move(postingsSec));

    constexpr uint16_t kVersion = 1;
    constexpr size_t kHeaderPad = 64;

    uint64_t cursor = kHeaderPad;
    for (auto& s : sections) {
        s.offset = cursor;
        cursor += s.payload.size();
    }
    const uint64_t tocOffset = cursor;

    std::string toc;
    for (const auto& s : sections) {
        writeLE(toc, s.id);
        writeLE(toc, s.encoding);
        writeLE(toc, s.offset);
        writeLE(toc, static_cast<uint64_t>(s.payload.size()));
        writeLE(toc, s.crc);
    }

    std::string header;
    header.append("SKD1", 4);
    writeLE(header, kVersion);
    writeLE(header, static_cast<uint16_t>(0));
    writeLE(header, static_cast<uint32_t>(chunk.docs.size()));
    writeLE(header, static_cast<uint32_t>(nextId));
    writeLE(header, tocOffset);
    writeLE(header, static_cast<uint32_t>(sections.size()));
    writeLE(header, static_cast<uint32_t>(0));
    uint32_t hdrCrc = minielastic::crc32(header);
    writeLE(header, hdrCrc);
    if (header.size() < kHeaderPad) {
        header.resize(kHeaderPad, '\0');
    }

    std::string file;
    file.reserve(header.size() + (cursor - kHeaderPad) + toc.size());
    file.append(header);
    for (const auto& s : sections) {
        file.append(s.payload);
    }
    file.append(toc);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(file.data(), static_cast<std::streamsize>(file.size()));
    return static_cast<bool>(out);
}

static bool readSnapshotFile(const std::string& path,
                             SnapshotChunk& outChunk,
                             uint32_t& nextId,
                             double& avgDocLen) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (data.size() < 64) return false;

    std::string_view view(data);
    if (std::memcmp(view.data(), "SKD1", 4) != 0) return false;

    size_t cursor = 4;
    uint16_t version = 0;
    uint16_t flags = 0;
    uint32_t docCount = 0;
    uint32_t nextIdLocal = 0;
    uint64_t tocOffset = 0;
    uint32_t tocEntries = 0;
    uint32_t reserved = 0;
    if (!readLE(view, cursor, version)) return false;
    if (!readLE(view, cursor, flags)) return false;
    if (!readLE(view, cursor, docCount)) return false;
    if (!readLE(view, cursor, nextIdLocal)) return false;
    if (!readLE(view, cursor, tocOffset)) return false;
    if (!readLE(view, cursor, tocEntries)) return false;
    if (!readLE(view, cursor, reserved)) return false;

    uint32_t storedHeaderCrc = 0;
    if (!readLE(view, cursor, storedHeaderCrc)) return false;
    const uint32_t computedHeaderCrc = minielastic::crc32(std::string_view(view.data(), cursor - sizeof(uint32_t)));
    if (storedHeaderCrc != computedHeaderCrc) return false;

    if (version != 1) return false;
    if (tocOffset >= view.size()) return false;

    struct TocEntry {
        uint16_t id;
        uint16_t encoding;
        uint64_t offset;
        uint64_t length;
        uint32_t crc;
    };

    std::map<uint16_t, TocEntry> toc;
    size_t tocCursor = tocOffset;
    for (uint32_t i = 0; i < tocEntries; ++i) {
        TocEntry e{};
        if (!readLE(view, tocCursor, e.id)) return false;
        if (!readLE(view, tocCursor, e.encoding)) return false;
        if (!readLE(view, tocCursor, e.offset)) return false;
        if (!readLE(view, tocCursor, e.length)) return false;
        if (!readLE(view, tocCursor, e.crc)) return false;
        if (e.offset + e.length > view.size()) return false;
        toc[e.id] = e;
    }

    auto getSection = [&](uint16_t id) -> std::string_view {
        auto it = toc.find(id);
        if (it == toc.end()) return {};
        const auto& e = it->second;
        return std::string_view(view.data() + e.offset, e.length);
    };

    const auto docTableView = getSection(3);
    const auto docBlobView = getSection(4);
    const auto docLensView = getSection(5);
    if (docTableView.empty() || docBlobView.data() == nullptr) return false;

    auto validateCrc = [&](uint16_t id, std::string_view section) {
        auto it = toc.find(id);
        if (it == toc.end()) return false;
        return minielastic::crc32(section) == it->second.crc;
    };

    if (!validateCrc(3, docTableView) || !validateCrc(4, docBlobView)) return false;
    if (!toc.empty()) {
        auto metaView = getSection(1);
        if (!metaView.empty() && validateCrc(1, metaView)) {
            auto parsed = json::parse(metaView, nullptr, false);
            if (!parsed.is_discarded()) {
                nextId = parsed.value("next_id", nextIdLocal);
                avgDocLen = parsed.value("avg_doc_len", avgDocLen);
            }
        }
    }

    const size_t entrySize = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    size_t dtCursor = 0;
    while (dtCursor + entrySize <= docTableView.size()) {
        uint32_t id{};
        uint16_t flagsValue{};
        uint64_t offset{};
        uint32_t len{};
        uint32_t checksum{};
        if (!readLE(docTableView, dtCursor, id)) break;
        if (!readLE(docTableView, dtCursor, flagsValue)) break;
        if (!readLE(docTableView, dtCursor, offset)) break;
        if (!readLE(docTableView, dtCursor, len)) break;
        if (!readLE(docTableView, dtCursor, checksum)) break;

        if (offset + len > docBlobView.size()) continue;
        std::string_view payload(docBlobView.data() + offset, len);
        if (minielastic::crc32(payload) != checksum) continue;

        auto j = json::from_cbor(payload, nullptr, false);
        if (j.is_discarded()) continue;

        outChunk.docs.push_back({id, j});
        nextId = std::max<uint32_t>(nextId, id + 1);
    }

    if (!docLensView.empty() && validateCrc(5, docLensView)) {
        size_t lc = 0;
        while (lc + sizeof(uint32_t) + sizeof(uint32_t) <= docLensView.size()) {
            uint32_t id{};
            uint32_t len{};
            if (!readLE(docLensView, lc, id)) break;
            if (!readLE(docLensView, lc, len)) break;
            outChunk.docLens[id] = len;
        }
    }

    const auto termDictView = getSection(6);
    const auto postingsView = getSection(7);
    if (!termDictView.empty() && !postingsView.empty() &&
        validateCrc(6, termDictView) && validateCrc(7, postingsView)) {
        size_t tdCursor = 0;
        while (tdCursor < termDictView.size()) {
            uint16_t termLen{};
            if (!readLE(termDictView, tdCursor, termLen)) break;
            if (tdCursor + termLen > termDictView.size()) break;
            std::string term(termDictView.substr(tdCursor, termLen));
            tdCursor += termLen;

            uint64_t off{};
            uint32_t len{};
            uint32_t df{};
            uint32_t checksum{};
            if (!readLE(termDictView, tdCursor, off)) break;
            if (!readLE(termDictView, tdCursor, len)) break;
            if (!readLE(termDictView, tdCursor, df)) break;
            if (!readLE(termDictView, tdCursor, checksum)) break;

            if (off + len > postingsView.size()) break;
            std::string_view plist(postingsView.data() + off, len);
            if (minielastic::crc32(plist) != checksum) continue;
            if (len % (sizeof(uint32_t) + sizeof(uint32_t)) != 0) continue;

            std::vector<minielastic::algo::Posting> postingsVec;
            size_t pcursor = 0;
            while (pcursor + sizeof(uint32_t) + sizeof(uint32_t) <= plist.size()) {
                uint32_t id{};
                uint32_t tf{};
                if (!readLE(plist, pcursor, id)) break;
                if (!readLE(plist, pcursor, tf)) break;
                postingsVec.push_back({id, tf});
            }
            if (!postingsVec.empty()) {
                outChunk.index[std::move(term)] = std::move(postingsVec);
            }
        }
    }

    return true;
}

} // namespace

namespace minielastic {

BlackBox::BlackBox(const std::string& dataDir) : dataDir_(dataDir) {
    if (!dataDir_.empty()) {
        std::filesystem::create_directories(dataDir_);
        loadSnapshot();
    }
}

BlackBox::~BlackBox() = default;

bool BlackBox::createIndex(const std::string& name, const IndexSchema& schema) {
    if (name.empty()) return false;
    if (indexes_.count(name)) return false;
    indexes_[name] = IndexState{};
    indexes_[name].schema = schema;
    return true;
}

bool BlackBox::indexExists(const std::string& name) const {
    return indexes_.count(name) > 0;
}

BlackBox::DocId BlackBox::indexDocument(const std::string& index, const std::string& jsonStr) {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) throw std::runtime_error("index not found");
    IndexState& idx = it->second;

    json j = json::parse(jsonStr);
    DocId id = idx.nextId++;
    idx.documents[id] = j;
    idx.docLengths[id] = 0;
    indexJson(idx, id, j);
    refreshAverages(idx);

    if (!dataDir_.empty()) saveSnapshot();
    return id;
}

nlohmann::json BlackBox::getDocument(const std::string& index, DocId id) const {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) throw std::runtime_error("index not found");
    const auto& docs = it->second.documents;
    auto d = docs.find(id);
    if (d == docs.end()) throw std::runtime_error("Document ID not found");
    return d->second;
}

bool BlackBox::deleteDocument(const std::string& index, DocId id) {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return false;
    IndexState& idx = it->second;
    auto d = idx.documents.find(id);
    if (d == idx.documents.end()) return false;
    removeJson(idx, id, d->second);
    idx.documents.erase(d);
    idx.docLengths.erase(id);
    refreshAverages(idx);
    if (!dataDir_.empty()) saveSnapshot();
    return true;
}

std::size_t BlackBox::documentCount(const std::string& index) const {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return 0;
    return it->second.documents.size();
}

std::vector<BlackBox::SearchHit> BlackBox::search(const std::string& index, const std::string& query, const std::string& mode, size_t maxResults, int maxEditDistance) const {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    const IndexState& idx = it->second;
    auto terms = tokenize(query);
    if (terms.empty()) return {};
    algo::SearchContext ctx{idx.documents, idx.invertedIndex, idx.docLengths, idx.avgDocLen};
    if (mode == "lexical") return algo::searchLexical(ctx, terms);
    if (mode == "fuzzy") return algo::searchFuzzy(ctx, terms, maxEditDistance, maxResults);
    if (mode == "semantic" || mode == "vector" || mode == "tfidf") {
        auto hits = algo::searchSemantic(ctx, terms, maxResults);
        if (!hits.empty()) return hits;
        return algo::searchBm25(ctx, terms, maxResults);
    }
    auto hits = algo::searchBm25(ctx, terms, maxResults);
    if (!hits.empty()) return hits;
    return algo::searchLexical(ctx, terms);
}

std::vector<BlackBox::SearchHit> BlackBox::searchHybrid(const std::string& index, const std::string& query, double wBm25, double wSemantic, double wLexical, size_t maxResults) const {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    const IndexState& idx = it->second;
    auto terms = tokenize(query);
    if (terms.empty()) return {};
    algo::SearchContext ctx{idx.documents, idx.invertedIndex, idx.docLengths, idx.avgDocLen};
    auto bm = algo::searchBm25(ctx, terms, maxResults * 2);
    auto sem = algo::searchSemantic(ctx, terms, maxResults * 2);
    auto lex = algo::searchLexical(ctx, terms);

    std::unordered_map<DocId, double> scores;
    auto blend = [&](const std::vector<SearchHit>& hits, double w) {
        for (const auto& h : hits) scores[h.id] += h.score * w;
    };
    blend(bm, wBm25);
    blend(sem, wSemantic);
    blend(lex, wLexical);

    std::vector<SearchHit> out;
    out.reserve(scores.size());
    for (const auto& kv : scores) out.push_back({kv.first, kv.second});
    std::sort(out.begin(), out.end(), [](const SearchHit& a, const SearchHit& b) {
        if (a.score == b.score) return a.id < b.id;
        return a.score > b.score;
    });
    if (out.size() > maxResults) out.resize(maxResults);
    return out;
}

void BlackBox::refreshAverages(IndexState& idx) {
    if (idx.docLengths.empty()) {
        idx.avgDocLen = 0.0;
        return;
    }
    uint64_t total = 0;
    for (const auto& kv : idx.docLengths) total += kv.second;
    idx.avgDocLen = static_cast<double>(total) / static_cast<double>(idx.docLengths.size());
}

std::vector<std::string> BlackBox::tokenize(const std::string& text) const {
    return Analyzer::tokenize(text);
}

void BlackBox::indexJson(IndexState& idx, DocId id, const json& j) {
    indexJsonRecursive(idx, id, j);
}

void BlackBox::indexJsonRecursive(IndexState& idx, DocId id, const json& node) {
    if (node.is_string()) {
        auto terms = tokenize(node.get<std::string>());
        if (!terms.empty()) {
            std::unordered_map<std::string, uint32_t> counts;
            for (const auto& t : terms) ++counts[t];
            idx.docLengths[id] += static_cast<uint32_t>(terms.size());
            for (const auto& kv : counts) {
                addPosting(idx, kv.first, id, kv.second);
            }
        }
    } else if (node.is_array()) {
        for (const auto& element : node) indexJsonRecursive(idx, id, element);
    } else if (node.is_object()) {
        for (const auto& it : node.items()) indexJsonRecursive(idx, id, it.value());
    }
}

void BlackBox::removeJson(IndexState& idx, DocId id, const json& j) {
    removeJsonRecursive(idx, id, j);
}

void BlackBox::removeJsonRecursive(IndexState& idx, DocId id, const json& node) {
    if (node.is_string()) {
        auto terms = Analyzer::tokenize(node.get<std::string>());
        if (!terms.empty()) {
            std::unordered_map<std::string, uint32_t> counts;
            for (const auto& t : terms) ++counts[t];
            auto lenIt = idx.docLengths.find(id);
            if (lenIt != idx.docLengths.end()) {
                uint32_t cur = lenIt->second;
                cur = cur > terms.size() ? cur - static_cast<uint32_t>(terms.size()) : 0;
                lenIt->second = cur;
            }
            for (const auto& term : counts) removePosting(idx, term.first, id);
        }
    } else if (node.is_array()) {
        for (const auto& element : node) removeJsonRecursive(idx, id, element);
    } else if (node.is_object()) {
        for (const auto& it : node.items()) removeJsonRecursive(idx, id, it.value());
    }
}

void BlackBox::addPosting(IndexState& idx, const std::string& term, DocId id, uint32_t tf) {
    auto& vec = idx.invertedIndex[term];
    if (!vec.empty() && vec.back().id == id) {
        vec.back().tf += tf;
        return;
    }
    vec.push_back({id, tf});
}

void BlackBox::removePosting(IndexState& idx, const std::string& term, DocId id) {
    auto it = idx.invertedIndex.find(term);
    if (it == idx.invertedIndex.end()) return;
    auto& vec = it->second;
    vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const algo::Posting& p) { return p.id == id; }), vec.end());
    if (vec.empty()) idx.invertedIndex.erase(it);
}

bool BlackBox::saveSnapshot(const std::string& path) const {
    namespace fs = std::filesystem;
    fs::path manifestPath = path.empty() ? fs::path(dataDir_) / "index.manifest" : fs::path(path);
    fs::create_directories(manifestPath.parent_path());

    json manifest = {{"version", 1}, {"indexes", json::array()}};
    for (const auto& entry : indexes_) {
        const auto& name = entry.first;
        const auto& idx = entry.second;
        SnapshotChunk chunk;
        chunk.docs.insert(chunk.docs.end(), idx.documents.begin(), idx.documents.end());
        chunk.docLens = idx.docLengths;
        chunk.index = idx.invertedIndex;

        fs::path shardFile = manifestPath.parent_path() / (name + ".skd");
        if (!writeSnapshotFile(shardFile.string(), chunk, idx.nextId, idx.avgDocLen)) {
            return false;
        }
        manifest["indexes"].push_back({{"name", name}, {"file", shardFile.filename().string()}, {"schema", idx.schema.schema}});
    }
    std::ofstream out(manifestPath, std::ios::binary | std::ios::trunc);
    out << manifest.dump(2);
    return static_cast<bool>(out);
}

bool BlackBox::loadSnapshot(const std::string& path) {
    namespace fs = std::filesystem;
    fs::path manifestPath = path.empty() ? fs::path(dataDir_) / "index.manifest" : fs::path(path);
    if (!fs::exists(manifestPath)) return false;
    std::ifstream in(manifestPath);
    if (!in) return false;
    json manifest = json::parse(in, nullptr, false);
    if (manifest.is_discarded()) return false;
    auto arr = manifest.value("indexes", json::array());
    if (!arr.is_array()) return false;

    indexes_.clear();

    for (const auto& idxJson : arr) {
        std::string name = idxJson.value("name", "");
        std::string file = idxJson.value("file", "");
        if (name.empty() || file.empty()) continue;
        fs::path shardPath = manifestPath.parent_path() / file;
        SnapshotChunk chunk;
        uint32_t nextIdTmp = 1;
        double avgTmp = 0.0;
        if (!readSnapshotFile(shardPath.string(), chunk, nextIdTmp, avgTmp)) continue;
        IndexState state;
        state.nextId = nextIdTmp;
        state.avgDocLen = avgTmp;
        state.schema.schema = idxJson.value("schema", json::object());
        for (const auto& kv : chunk.docs) state.documents[kv.first] = kv.second;
        state.docLengths = std::move(chunk.docLens);
        state.invertedIndex = std::move(chunk.index);
        refreshAverages(state);
        indexes_[name] = std::move(state);
    }
    return true;
}

} // namespace minielastic
