//BlackBox.cpp
#include "BlackBox.hpp"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string_view>
#include <sstream>
#include <cstring>
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
    std::unordered_map<uint32_t, std::vector<float>> vectors;
    uint32_t vectorDim = 0;
    json docValues; // numeric, bool, string lists
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
        {"avg_doc_len", avgDocLen},
        {"vector_dim", chunk.vectorDim}
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
        uint32_t prev = 0;
        for (const auto& p : postingList) {
            uint32_t delta = p.id - prev;
            prev = p.id;
            writeLE(postingBytes, delta);
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

    // Section 8: vectors (id + floats)
    std::string vecPayload;
    if (chunk.vectorDim > 0) {
        for (const auto& kv : chunk.vectors) {
            writeLE(vecPayload, kv.first);
            for (uint32_t i = 0; i < chunk.vectorDim && i < kv.second.size(); ++i) {
                const float f = kv.second[i];
                const uint32_t raw = *reinterpret_cast<const uint32_t*>(&f);
                writeLE(vecPayload, raw);
            }
        }
    }
    Section vecSec = makeSection(8, std::move(vecPayload));

    // Section 9: doc-values (JSON)
    Section dvSec = makeSection(9, chunk.docValues.dump());

    std::vector<Section> sections;
    sections.push_back(std::move(metaSec));
    sections.push_back(std::move(docTableSec));
    sections.push_back(std::move(docBlobSec));
    sections.push_back(std::move(docLenSec));
    sections.push_back(std::move(termDictSec));
    sections.push_back(std::move(postingsSec));
    sections.push_back(std::move(vecSec));
    sections.push_back(std::move(dvSec));

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
                outChunk.vectorDim = parsed.value("vector_dim", outChunk.vectorDim);
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
    const auto vecView = getSection(8);
    const auto dvView = getSection(9);
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
            uint32_t accId = 0;
            while (pcursor + sizeof(uint32_t) + sizeof(uint32_t) <= plist.size()) {
                uint32_t delta{};
                uint32_t tf{};
                if (!readLE(plist, pcursor, delta)) break;
                if (!readLE(plist, pcursor, tf)) break;
                accId += delta;
                postingsVec.push_back({accId, tf});
            }
            if (!postingsVec.empty()) {
                outChunk.index[std::move(term)] = std::move(postingsVec);
            }
        }
    }

    // Vectors
    if (outChunk.vectorDim > 0 && !vecView.empty() && validateCrc(8, vecView)) {
        size_t vc = 0;
        while (vc + sizeof(uint32_t) * (1 + outChunk.vectorDim) <= vecView.size()) {
            uint32_t id{};
            if (!readLE(vecView, vc, id)) break;
            std::vector<float> vec(outChunk.vectorDim, 0.0f);
            for (uint32_t i = 0; i < outChunk.vectorDim; ++i) {
                uint32_t raw{};
                if (!readLE(vecView, vc, raw)) { vec.clear(); break; }
                float f;
                std::memcpy(&f, &raw, sizeof(float));
                vec[i] = f;
            }
            if (!vec.empty()) outChunk.vectors[id] = std::move(vec);
        }
    }

    // Doc-values
    if (!dvView.empty() && validateCrc(9, dvView)) {
        auto dv = json::parse(dvView, nullptr, false);
        if (!dv.is_discarded()) {
            outChunk.docValues = dv;
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
    // Parse field types
    if (schema.schema.contains("fields") && schema.schema["fields"].is_object()) {
        for (auto it = schema.schema["fields"].begin(); it != schema.schema["fields"].end(); ++it) {
            if (it.value().is_string()) {
                auto t = it.value().get<std::string>();
                if (t == "text") indexes_[name].schema.fieldTypes[it.key()] = FieldType::Text;
                else if (t == "array") indexes_[name].schema.fieldTypes[it.key()] = FieldType::ArrayString;
                else if (t == "bool") indexes_[name].schema.fieldTypes[it.key()] = FieldType::Bool;
                else if (t == "number") indexes_[name].schema.fieldTypes[it.key()] = FieldType::Number;
            } else if (it.value().is_object()) {
                auto type = it.value().value("type", "");
                if (type == "vector") {
                    indexes_[name].schema.vectorField = it.key();
                    indexes_[name].schema.vectorDim = it.value().value("dim", 0);
                    indexes_[name].schema.fieldTypes[it.key()] = FieldType::Vector;
                }
            }
        }
    }
    return true;
}

bool BlackBox::indexExists(const std::string& name) const {
    return indexes_.count(name) > 0;
}

const BlackBox::IndexSchema* BlackBox::getSchema(const std::string& name) const {
    auto it = indexes_.find(name);
    if (it == indexes_.end()) return nullptr;
    return &it->second.schema;
}

BlackBox::DocId BlackBox::indexDocument(const std::string& index, const std::string& jsonStr) {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) throw std::runtime_error("index not found");
    IndexState& idx = it->second;

    json j = json::parse(jsonStr);
    if (!validateDocument(idx, j)) {
        throw std::runtime_error("document does not conform to schema");
    }

    // Handle vector extraction
    if (!idx.schema.vectorField.empty() && idx.schema.vectorDim > 0) {
        auto vf = idx.schema.vectorField;
        if (j.contains(vf)) {
            const auto& arr = j[vf];
            std::vector<float> vec;
            if (arr.is_array()) {
                for (size_t i = 0; i < idx.schema.vectorDim && i < arr.size(); ++i) {
                    vec.push_back(static_cast<float>(arr[i].get<double>()));
                }
                if (vec.size() == idx.schema.vectorDim) {
                    idx.vectors[idx.nextId] = vec;
                }
            }
        }
    }

    DocId id = idx.nextId++;
    idx.documents[id] = j;
    idx.docLengths[id] = 0;
    indexStructured(idx, id, j);
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
    indexStructured(idx, id, json::object()); // reset length
    removeJson(idx, id, d->second);
    idx.documents.erase(d);
    idx.docLengths.erase(id);
    idx.vectors.erase(id);
    for (auto& kv : idx.boolValues) kv.second.erase(id);
    for (auto& kv : idx.numericValues) kv.second.erase(id);
    for (auto& kv : idx.stringLists) {
        for (auto& bucket : kv.second) {
            auto& vec = bucket.second;
            vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
        }
    }
    refreshAverages(idx);
    if (!dataDir_.empty()) saveSnapshot();
    return true;
}

bool BlackBox::updateDocument(const std::string& index, DocId id, const std::string& jsonStr, bool partial) {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return false;
    IndexState& idx = it->second;
    auto existing = idx.documents.find(id);
    if (existing == idx.documents.end()) return false;

    json incoming = json::parse(jsonStr);
    json merged = partial ? existing->second : json::object();
    if (partial && incoming.is_object()) {
        for (auto itf = incoming.begin(); itf != incoming.end(); ++itf) {
            merged[itf.key()] = itf.value();
        }
    } else {
        merged = incoming;
    }

    if (!validateDocument(idx, merged)) {
        throw std::runtime_error("document does not conform to schema");
    }

    // remove old postings/vectors
    removeJson(idx, id, existing->second);
    idx.docLengths.erase(id);
    idx.vectors.erase(id);

    // apply vector
    if (!idx.schema.vectorField.empty() && idx.schema.vectorDim > 0) {
        auto vf = idx.schema.vectorField;
        if (merged.contains(vf)) {
            const auto& arr = merged[vf];
            std::vector<float> vec;
            if (arr.is_array()) {
                for (size_t i = 0; i < idx.schema.vectorDim && i < arr.size(); ++i) {
                    vec.push_back(static_cast<float>(arr[i].get<double>()));
                }
                if (vec.size() == idx.schema.vectorDim) {
                    idx.vectors[id] = vec;
                }
            }
        }
    }

    idx.documents[id] = merged;
    indexStructured(idx, id, merged);
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

std::vector<BlackBox::SearchHit> BlackBox::searchVector(const std::string& index, const std::vector<float>& queryVec, size_t maxResults) const {
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    const IndexState& idx = it->second;
    if (idx.schema.vectorDim == 0 || queryVec.size() != idx.schema.vectorDim) return {};

    auto dot = [](const std::vector<float>& a, const std::vector<float>& b) {
        double s = 0.0;
        for (size_t i = 0; i < a.size(); ++i) s += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        return s;
    };
    auto norm = [](const std::vector<float>& a) {
        double s = 0.0;
        for (auto v : a) s += static_cast<double>(v) * static_cast<double>(v);
        return std::sqrt(s);
    };
    double qn = norm(queryVec);
    if (qn == 0) return {};

    std::vector<SearchHit> hits;
    for (const auto& kv : idx.vectors) {
        const auto& vec = kv.second;
        if (vec.size() != idx.schema.vectorDim) continue;
        double score = dot(queryVec, vec) / (qn * norm(vec) + 1e-9);
        hits.push_back({kv.first, score});
    }
    std::sort(hits.begin(), hits.end(), [](const SearchHit& a, const SearchHit& b) {
        if (a.score == b.score) return a.id < b.id;
        return a.score > b.score;
    });
    if (hits.size() > maxResults) hits.resize(maxResults);
    return hits;
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
    if (idx.schema.fieldTypes.empty()) {
        indexJsonRecursive(idx, id, j);
    } else {
        indexStructured(idx, id, j);
    }
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

void BlackBox::indexStructured(IndexState& idx, DocId id, const json& doc) {
    idx.docLengths[id] = 0;
    for (const auto& ft : idx.schema.fieldTypes) {
        const auto& key = ft.first;
        auto type = ft.second;
        if (!doc.contains(key)) continue;
        const auto& val = doc[key];
        if (type == FieldType::Text && val.is_string()) {
            auto terms = tokenize(val.get<std::string>());
            idx.docLengths[id] += static_cast<uint32_t>(terms.size());
            for (const auto& t : terms) addPosting(idx, t, id, 1);
        } else if (type == FieldType::ArrayString && val.is_array()) {
            for (const auto& elem : val) {
                if (elem.is_string()) {
                    auto terms = tokenize(elem.get<std::string>());
                    idx.docLengths[id] += static_cast<uint32_t>(terms.size());
                    for (const auto& t : terms) addPosting(idx, t, id, 1);
                    idx.stringLists[key][elem.get<std::string>()].push_back(id);
                }
            }
        } else if (type == FieldType::Vector) {
            // handled separately during ingest
            continue;
        } else if (type == FieldType::Bool && val.is_boolean()) {
            idx.boolValues[key][id] = val.get<bool>();
        } else if (type == FieldType::Number && val.is_number()) {
            idx.numericValues[key][id] = val.get<double>();
        }
    }
}

void BlackBox::removeJson(IndexState& idx, DocId id, const json& j) {
    // Remove postings based on schema-defined fields
    if (idx.schema.fieldTypes.empty()) {
        removeJsonRecursive(idx, id, j);
        return;
    }
    for (const auto& ft : idx.schema.fieldTypes) {
        const auto& key = ft.first;
        auto type = ft.second;
        if (!j.contains(key)) continue;
        const auto& val = j[key];
        if (type == FieldType::Text && val.is_string()) {
            auto terms = tokenize(val.get<std::string>());
            for (const auto& t : terms) removePosting(idx, t, id);
        } else if (type == FieldType::ArrayString && val.is_array()) {
            for (const auto& elem : val) {
                if (elem.is_string()) {
                    auto terms = tokenize(elem.get<std::string>());
                    for (const auto& t : terms) removePosting(idx, t, id);
                }
            }
        }
    }
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

bool BlackBox::validateDocument(const IndexState& idx, const nlohmann::json& doc) const {
    for (const auto& ft : idx.schema.fieldTypes) {
        const auto& key = ft.first;
        auto type = ft.second;
        if (!doc.contains(key)) continue; // optional
        const auto& val = doc[key];
        switch (type) {
        case FieldType::Text:
            if (!val.is_string()) return false;
            break;
        case FieldType::ArrayString:
            if (!val.is_array()) return false;
            for (const auto& e : val) {
                if (!e.is_string()) return false;
            }
            break;
        case FieldType::Bool:
            if (!val.is_boolean()) return false;
            break;
        case FieldType::Number:
            if (!val.is_number()) return false;
            break;
        case FieldType::Vector:
            if (!val.is_array()) return false;
            if (val.size() < idx.schema.vectorDim) return false;
            break;
        default:
            break;
        }
    }
    return true;
}

bool BlackBox::saveSnapshot(const std::string& path) const {
    namespace fs = std::filesystem;
    fs::path manifestPath = path.empty() ? fs::path(dataDir_) / "index.manifest" : fs::path(path);
    fs::create_directories(manifestPath.parent_path());

    json manifest = {{"version", 1}, {"indexes", json::array()}};
    for (const auto& entry : indexes_) {
        const auto& name = entry.first;
        const auto& idx = entry.second;
        // Segmentize docs in chunks to allow future incremental writes
        std::vector<uint32_t> ids;
        ids.reserve(idx.documents.size());
        for (const auto& kv : idx.documents) ids.push_back(kv.first);
        std::sort(ids.begin(), ids.end());
        constexpr size_t kSegmentSize = 5000;
        json segs = json::array();
        for (size_t start = 0; start < ids.size(); start += kSegmentSize) {
            size_t end = std::min(ids.size(), start + kSegmentSize);
            SnapshotChunk chunk;
            chunk.vectorDim = idx.schema.vectorDim;
            // Build chunk by re-indexing documents in this slice
            minielastic::BlackBox::IndexState tmp;
            tmp.schema = idx.schema;
            for (size_t i = start; i < end; ++i) {
                uint32_t id = ids[i];
                auto itDoc = idx.documents.find(id);
                if (itDoc == idx.documents.end()) continue;
                tmp.nextId = std::max(tmp.nextId, id + 1);
                tmp.documents[id] = itDoc->second;
                tmp.docLengths[id] = 0;
                const_cast<BlackBox*>(this)->indexStructured(tmp, id, itDoc->second);
                auto vecIt = idx.vectors.find(id);
                if (vecIt != idx.vectors.end()) tmp.vectors[id] = vecIt->second;
                // carry numeric/bool/string doc values
                for (const auto& n : idx.numericValues) {
                    auto itv = n.second.find(id);
                    if (itv != n.second.end()) tmp.numericValues[n.first][id] = itv->second;
                }
                for (const auto& b : idx.boolValues) {
                    auto itv = b.second.find(id);
                    if (itv != b.second.end()) tmp.boolValues[b.first][id] = itv->second;
                }
                for (const auto& s : idx.stringLists) {
                    for (const auto& bucket : s.second) {
                        if (std::find(bucket.second.begin(), bucket.second.end(), id) != bucket.second.end()) {
                            tmp.stringLists[s.first][bucket.first].push_back(id);
                        }
                    }
                }
            }
            chunk.docs.insert(chunk.docs.end(), tmp.documents.begin(), tmp.documents.end());
            chunk.docLens = tmp.docLengths;
            chunk.index = tmp.invertedIndex;
            chunk.vectors = tmp.vectors;
            chunk.docValues = json{
                {"numeric", tmp.numericValues},
                {"bool", tmp.boolValues},
                {"strings", tmp.stringLists}
            };
            double avg = tmp.docLengths.empty() ? 0.0 : [&]() {
                uint64_t total = 0;
                for (const auto& kv : tmp.docLengths) total += kv.second;
                return static_cast<double>(total) / static_cast<double>(tmp.docLengths.size());
            }();

            fs::path shardFile = manifestPath.parent_path() / (name + "_seg" + std::to_string(segs.size()) + ".skd");
            if (!writeSnapshotFile(shardFile.string(), chunk, tmp.nextId, avg)) {
                return false;
            }
            segs.push_back({{"file", shardFile.filename().string()}});
        }
        manifest["indexes"].push_back({
            {"name", name},
            {"segments", segs},
            {"schema", idx.schema.schema}
        });
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
        if (name.empty()) continue;
        json segments = idxJson.value("segments", json::array());
        if (!segments.is_array() || segments.empty()) {
            // backward compatibility: single file entry
            std::string file = idxJson.value("file", "");
            if (file.empty()) continue;
            segments = json::array({json{{"file", file}}});
        }

        IndexState state;
        state.schema.schema = idxJson.value("schema", json::object());
        state.schema.fieldTypes.clear();
        if (state.schema.schema.contains("fields") && state.schema.schema["fields"].is_object()) {
            auto fields = state.schema.schema["fields"];
            for (auto it = fields.begin(); it != fields.end(); ++it) {
                if (it.value().is_string()) {
                    auto t = it.value().get<std::string>();
                    if (t == "text") state.schema.fieldTypes[it.key()] = FieldType::Text;
                    else if (t == "array") state.schema.fieldTypes[it.key()] = FieldType::ArrayString;
                    else if (t == "bool") state.schema.fieldTypes[it.key()] = FieldType::Bool;
                    else if (t == "number") state.schema.fieldTypes[it.key()] = FieldType::Number;
                } else if (it.value().is_object()) {
                    auto type = it.value().value("type", "");
                    if (type == "vector") {
                        state.schema.vectorField = it.key();
                        state.schema.vectorDim = it.value().value("dim", 0);
                        state.schema.fieldTypes[it.key()] = FieldType::Vector;
                    }
                }
            }
        }

        for (const auto& seg : segments) {
            std::string file = seg.value("file", "");
            if (file.empty()) continue;
            fs::path shardPath = manifestPath.parent_path() / file;
            SnapshotChunk chunk;
            uint32_t nextIdTmp = 1;
            double avgTmp = 0.0;
            if (!readSnapshotFile(shardPath.string(), chunk, nextIdTmp, avgTmp)) continue;
            state.nextId = std::max(state.nextId, nextIdTmp);
            state.avgDocLen = avgTmp;
            state.schema.vectorDim = chunk.vectorDim;
            for (const auto& kv : chunk.docs) state.documents[kv.first] = kv.second;
            for (const auto& kv : chunk.docLens) state.docLengths[kv.first] = kv.second;
            for (const auto& kv : chunk.index) {
                auto& dest = state.invertedIndex[kv.first];
                dest.insert(dest.end(), kv.second.begin(), kv.second.end());
            }
            for (const auto& kv : chunk.vectors) {
                state.vectors[kv.first] = kv.second;
            }
            if (chunk.docValues.contains("numeric")) {
                auto num = chunk.docValues["numeric"];
                if (num.is_object()) {
                    for (auto itn = num.begin(); itn != num.end(); ++itn) {
                        for (auto itv = itn->begin(); itv != itn->end(); ++itv) {
                            uint32_t id = std::stoul(itv.key());
                            state.numericValues[itn.key()][id] = itv.value().get<double>();
                        }
                    }
                }
            }
            if (chunk.docValues.contains("bool")) {
                auto bl = chunk.docValues["bool"];
                if (bl.is_object()) {
                    for (auto itb = bl.begin(); itb != bl.end(); ++itb) {
                        for (auto itv = itb->begin(); itv != itb->end(); ++itv) {
                            uint32_t id = std::stoul(itv.key());
                            state.boolValues[itb.key()][id] = itv.value().get<bool>();
                        }
                    }
                }
            }
            if (chunk.docValues.contains("strings")) {
                auto st = chunk.docValues["strings"];
                if (st.is_object()) {
                    for (auto its = st.begin(); its != st.end(); ++its) {
                        for (auto itb = its->begin(); itb != its->end(); ++itb) {
                            if (!itb.value().is_array()) continue;
                            std::vector<DocId> idsArr;
                            for (const auto& val : itb.value()) idsArr.push_back(val.get<uint32_t>());
                            state.stringLists[its.key()][itb.key()] = std::move(idsArr);
                        }
                    }
                }
            }
        }

        // Ensure postings are sorted/unique
        for (auto& termEntry : state.invertedIndex) {
            auto& vec = termEntry.second;
            std::sort(vec.begin(), vec.end(), [](const algo::Posting& a, const algo::Posting& b) { return a.id < b.id; });
            vec.erase(std::unique(vec.begin(), vec.end(), [](const algo::Posting& a, const algo::Posting& b){return a.id==b.id;}), vec.end());
        }

        refreshAverages(state);
        indexes_[name] = std::move(state);
    }
    return true;
}

} // namespace minielastic
