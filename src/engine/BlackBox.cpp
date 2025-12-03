//BlackBox.cpp
#include "BlackBox.hpp"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <unordered_set>
#include <cstdlib>
#include <stdexcept>
#include <string_view>
#include <sstream>
#include <cstring>
#include <iostream>
#include <optional>
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

template <typename T>
void walWriteLE(std::ostream& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        char byte = static_cast<char>((static_cast<uint64_t>(value) >> (8 * i)) & 0xFFu);
        out.write(&byte, 1);
    }
}

template <typename T>
bool walReadLE(std::istream& in, T& value) {
    value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        char byte = 0;
        if (!in.read(&byte, 1)) return false;
        value |= static_cast<uint64_t>(static_cast<unsigned char>(byte)) << (8 * i);
    }
    return true;
}

enum class SectionEncoding : uint16_t { Raw = 0, Zstd = 1 };

#ifdef BLACKBOX_USE_ZSTD
#include <zstd.h>
static bool compressZstd(const std::string& in, std::string& out, int level = 3) {
    size_t maxSize = ZSTD_compressBound(in.size());
    out.resize(maxSize);
    size_t written = ZSTD_compress(out.data(), maxSize, in.data(), in.size(), level);
    if (ZSTD_isError(written)) return false;
    out.resize(written);
    return true;
}
static bool decompressZstd(std::string_view in, std::string& out) {
    unsigned long long rawSize = ZSTD_getFrameContentSize(in.data(), in.size());
    if (rawSize == ZSTD_CONTENTSIZE_ERROR || rawSize == ZSTD_CONTENTSIZE_UNKNOWN) return false;
    out.resize(static_cast<size_t>(rawSize));
    size_t res = ZSTD_decompress(out.data(), rawSize, in.data(), in.size());
    if (ZSTD_isError(res)) return false;
    out.resize(res);
    return true;
}
#endif

static uint16_t maybeCompress(std::string& payload, bool enable) {
    if (!enable) return static_cast<uint16_t>(SectionEncoding::Raw);
#ifdef BLACKBOX_USE_ZSTD
    std::string compressed;
    if (compressZstd(payload, compressed)) {
        payload.swap(compressed);
        return static_cast<uint16_t>(SectionEncoding::Zstd);
    }
#endif
    return static_cast<uint16_t>(SectionEncoding::Raw);
}

static bool maybeDecompress(std::string_view in, uint16_t encoding, std::string& out) {
    if (encoding == static_cast<uint16_t>(SectionEncoding::Raw)) {
        out.assign(in.begin(), in.end());
        return true;
    }
#ifdef BLACKBOX_USE_ZSTD
    if (encoding == static_cast<uint16_t>(SectionEncoding::Zstd)) {
        return decompressZstd(in, out);
    }
#endif
    std::cerr << "Snapshot: unsupported encoding=" << encoding << "\n";
    return false;
}

static bool writeSnapshotFile(const std::string& path,
                              const SnapshotChunk& chunk,
                              uint32_t nextId,
                              double avgDocLen,
                              bool compressSections) {
    struct Section {
        uint16_t id;
        uint16_t encoding;
        uint64_t offset = 0;
        std::string payload;
        uint32_t crc = 0;
    };

    auto makeSection = [](uint16_t id, std::string payload, bool compress) {
        Section s{id, 0, 0, std::move(payload), 0};
        s.encoding = maybeCompress(s.payload, compress);
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
    Section metaSec = makeSection(1, meta.dump(), false);

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
    Section docTableSec = makeSection(3, std::move(docTable), compressSections);
    Section docBlobSec = makeSection(4, std::move(docBlob), compressSections);

    std::string docLensPayload;
    for (const auto& kv : chunk.docLens) {
        writeLE(docLensPayload, kv.first);
        writeLE(docLensPayload, static_cast<uint32_t>(kv.second));
    }
    Section docLenSec = makeSection(5, std::move(docLensPayload), compressSections);

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

    Section termDictSec = makeSection(6, std::move(termDict), compressSections);
    Section postingsSec = makeSection(7, std::move(postings), compressSections);

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
    Section vecSec = makeSection(8, std::move(vecPayload), compressSections);

    // Section 9: doc-values (JSON)
    Section dvSec = makeSection(9, chunk.docValues.dump(), compressSections);

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

    auto getSection = [&](uint16_t id) -> std::optional<std::pair<std::string_view, uint16_t>> {
        auto it = toc.find(id);
        if (it == toc.end()) return std::nullopt;
        const auto& e = it->second;
        return std::make_pair(std::string_view(view.data() + e.offset, e.length), e.encoding);
    };

    const auto docTableSec = getSection(3);
    const auto docBlobSec = getSection(4);
    const auto docLensSec = getSection(5);
    if (!docTableSec || !docBlobSec) return false;
    const auto docTableViewCompressed = docTableSec->first;
    const auto docBlobViewCompressed = docBlobSec->first;
    std::string docTableDecoded;
    std::string docBlobViewStorage;
    std::string docLensStorage;
    const std::string_view docLensViewRaw = docLensSec ? docLensSec->first : std::string_view();
    std::string_view docLensView = docLensViewRaw;
    if (docLensSec && docLensSec->second != static_cast<uint16_t>(SectionEncoding::Raw)) {
        if (!maybeDecompress(docLensViewRaw, docLensSec->second, docLensStorage)) return false;
        docLensView = std::string_view(docLensStorage);
    }

    auto validateCrc = [&](uint16_t id, std::string_view section) {
        auto it = toc.find(id);
        if (it == toc.end()) return false;
        return minielastic::crc32(section) == it->second.crc;
    };

    if (!validateCrc(3, docTableViewCompressed) || !validateCrc(4, docBlobViewCompressed)) return false;
    if (!toc.empty()) {
        auto metaSec = getSection(1);
        if (metaSec && validateCrc(1, metaSec->first)) {
            auto parsed = json::parse(metaSec->first, nullptr, false);
            if (!parsed.is_discarded()) {
                nextId = parsed.value("next_id", nextIdLocal);
                avgDocLen = parsed.value("avg_doc_len", avgDocLen);
                outChunk.vectorDim = parsed.value("vector_dim", outChunk.vectorDim);
            }
        }
    }

    // Decompress doc table if needed
    std::string_view docTableView = docTableViewCompressed;
    if (docTableSec->second != static_cast<uint16_t>(SectionEncoding::Raw)) {
        if (!maybeDecompress(docTableViewCompressed, docTableSec->second, docTableDecoded)) return false;
        docTableView = std::string_view(docTableDecoded);
    }

    // Decompress doc blob if needed
    std::string docBlobDecoded;
    if (docBlobSec->second == static_cast<uint16_t>(SectionEncoding::Raw)) {
        docBlobDecoded.assign(docBlobViewCompressed.begin(), docBlobViewCompressed.end());
    } else {
        if (!maybeDecompress(docBlobViewCompressed, docBlobSec->second, docBlobDecoded)) return false;
    }
    std::string_view docBlobView(docBlobDecoded);

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

    if (docLensSec && !docLensView.empty() && validateCrc(5, docLensSec->first)) {
        size_t lc = 0;
        while (lc + sizeof(uint32_t) + sizeof(uint32_t) <= docLensView.size()) {
            uint32_t id{};
            uint32_t len{};
            if (!readLE(docLensView, lc, id)) break;
            if (!readLE(docLensView, lc, len)) break;
            outChunk.docLens[id] = len;
        }
    }

    const auto termDictSec = getSection(6);
    const auto postingsSec = getSection(7);
    const auto vecSec = getSection(8);
    const auto dvSec = getSection(9);

    std::string termDictDecoded;
    std::string postingsDecoded;
    if (termDictSec && postingsSec &&
        validateCrc(6, termDictSec->first) && validateCrc(7, postingsSec->first)) {
        if (termDictSec->second == static_cast<uint16_t>(SectionEncoding::Raw)) {
            termDictDecoded.assign(termDictSec->first.begin(), termDictSec->first.end());
        } else {
            if (!maybeDecompress(termDictSec->first, termDictSec->second, termDictDecoded)) return false;
        }
        if (postingsSec->second == static_cast<uint16_t>(SectionEncoding::Raw)) {
            postingsDecoded.assign(postingsSec->first.begin(), postingsSec->first.end());
        } else {
            if (!maybeDecompress(postingsSec->first, postingsSec->second, postingsDecoded)) return false;
        }
        std::string_view termDictView(termDictDecoded);
        std::string_view postingsView(postingsDecoded);
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
    if (outChunk.vectorDim > 0 && vecSec && validateCrc(8, vecSec->first)) {
        std::string vecDecoded;
        std::string_view vecView;
        if (vecSec->second == static_cast<uint16_t>(SectionEncoding::Raw)) {
            vecDecoded.assign(vecSec->first.begin(), vecSec->first.end());
        } else {
            if (!maybeDecompress(vecSec->first, vecSec->second, vecDecoded)) return false;
        }
        vecView = std::string_view(vecDecoded);
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
    if (dvSec && validateCrc(9, dvSec->first)) {
        std::string dvDecoded;
        std::string_view dvView;
        if (dvSec->second == static_cast<uint16_t>(SectionEncoding::Raw)) {
            dvDecoded.assign(dvSec->first.begin(), dvSec->first.end());
        } else {
            if (!maybeDecompress(dvSec->first, dvSec->second, dvDecoded)) return false;
        }
        dvView = std::string_view(dvDecoded);
        if (!dvView.empty()) {
            auto dv = json::parse(dvView, nullptr, false);
            if (!dv.is_discarded()) {
                outChunk.docValues = dv;
            }
        }
    }

    return true;
}

} // namespace

namespace minielastic {

bool WalWriter::open() {
    if (path.empty()) return false;
    try {
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    } catch (...) {
        std::cerr << "WalWriter: failed to create dir for " << path << "\n";
    }
    stream.open(path, std::ios::binary | std::ios::app | std::ios::out);
    if (!stream) {
        std::cerr << "WalWriter: failed to open " << path << "\n";
        return false;
    }
    stream.seekp(0, std::ios::end);
    offset = static_cast<uint64_t>(stream.tellp());
    return true;
}

void WalWriter::close() {
    if (stream.is_open()) stream.close();
}

bool WalWriter::append(const WalRecord& rec) {
    if (!stream.is_open()) return false;
    if (!stream.good()) {
        std::cerr << "WalWriter: stream not good for " << path << "\n";
        return false;
    }
    stream.write(reinterpret_cast<const char*>(&rec.op), sizeof(rec.op));
    walWriteLE(stream, rec.docId);
    uint32_t len = static_cast<uint32_t>(rec.payload.size());
    walWriteLE(stream, len);
    if (!rec.payload.empty()) stream.write(rec.payload.data(), static_cast<std::streamsize>(rec.payload.size()));
    stream.flush();
    offset += sizeof(rec.op) + sizeof(uint32_t) + sizeof(uint32_t) + len;
    if (!stream) {
        std::cerr << "WalWriter: stream write failed for " << path << "\n";
    }
    return static_cast<bool>(stream);
}

void WalWriter::reset() {
    close();
    if (!path.empty()) {
        try {
            std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        } catch (...) {
            std::cerr << "WalWriter: failed to create dir for " << path << "\n";
        }
        // truncate file
        std::ofstream truncStream(path, std::ios::binary | std::ios::trunc | std::ios::out);
        if (!truncStream) {
            std::cerr << "WalWriter: failed to truncate " << path << "\n";
        }
        truncStream.close();
        offset = 0;
    }
}

std::vector<WalRecord> readWalRecords(const std::string& path) {
    std::vector<WalRecord> out;
    std::ifstream in(path, std::ios::binary);
    if (!in) return out;
    while (true) {
        WalOp op;
        if (!in.read(reinterpret_cast<char*>(&op), sizeof(op))) break;
        uint32_t docId = 0;
        if (!walReadLE(in, docId)) break;
        uint32_t len = 0;
        if (!walReadLE(in, len)) break;
        std::vector<char> payload(len);
        if (len > 0) {
            if (!in.read(payload.data(), len)) break;
        }
        out.push_back({op, docId, std::move(payload)});
    }
    return out;
}

BlackBox::BlackBox(const std::string& dataDir) : dataDir_(dataDir) {
    // Configure tunables via environment
    if (const char* envFlush = std::getenv("BLACKBOX_FLUSH_DOCS")) {
        try { flushEveryDocs_ = std::max<size_t>(1, static_cast<size_t>(std::stoull(envFlush))); } catch (...) {}
    }
    if (const char* envComp = std::getenv("BLACKBOX_COMPRESS")) {
        std::string v(envComp);
        compressSnapshots_ = !(v == "0" || v == "false" || v == "off");
    }
    if (const char* envAnn = std::getenv("BLACKBOX_ANN_CLUSTERS")) {
        try { defaultAnnClusters_ = static_cast<uint32_t>(std::max<uint64_t>(1, std::stoull(envAnn))); } catch (...) {}
    }
    if (const char* envMerge = std::getenv("BLACKBOX_MERGE_SEGMENTS")) {
        try { mergeSegmentsAt_ = std::max<size_t>(1, static_cast<size_t>(std::stoull(envMerge))); } catch (...) {}
    }

    if (!dataDir_.empty()) {
        namespace fs = std::filesystem;
        fs::path dataPath = fs::absolute(fs::path(dataDir_));
        dataDir_ = dataPath.string();
        fs::create_directories(dataDir_);
        std::cerr << "BlackBox: dataDir=" << dataDir_ << " flushEveryDocs=" << flushEveryDocs_ << " mergeSegmentsAt=" << mergeSegmentsAt_ << " compress=" << (compressSnapshots_ ? "on" : "off") << " annClusters=" << defaultAnnClusters_ << "\n";
        bool loaded = loadSnapshot();
        if (!loaded) {
            std::cerr << "BlackBox: loadSnapshot failed, trying WAL only\n";
            loadWalOnly();
        } else {
            std::cerr << "BlackBox: loadSnapshot succeeded\n";
        }
        // open WALs for existing indexes
        for (auto& kv : indexes_) {
            if (kv.second.wal.path.empty()) {
                kv.second.wal.path = (std::filesystem::path(dataDir_) / (kv.first + ".wal")).string();
            }
            if (!kv.second.wal.stream.is_open()) {
                kv.second.wal.open();
            }
            if (kv.second.wal.stream.is_open()) {
                replayWal(kv.second);
                std::cerr << "BlackBox: replayed WAL for index " << kv.first << "\n";
            } else {
                std::cerr << "WalWriter: failed to open " << kv.second.wal.path << "\n";
            }
        }
        std::cerr << "BlackBox: init complete; indexes=" << indexes_.size() << "\n";
    }
}

BlackBox::~BlackBox() {
    writeManifest();
}

bool BlackBox::createIndex(const std::string& name, const IndexSchema& schema) {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    if (name.empty()) return false;
    if (indexes_.count(name)) return false;
    indexes_[name] = IndexState{};
    indexes_[name].schema = schema;
    indexes_[name].annClusters = defaultAnnClusters_;
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
    // init WAL
    if (!dataDir_.empty()) {
        indexes_[name].wal.path = (std::filesystem::path(dataDir_) / (name + ".wal")).string();
        indexes_[name].wal.reset();
        indexes_[name].wal.open();
        writeManifest();
    }
    return true;
}

bool BlackBox::indexExists(const std::string& name) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    return indexes_.count(name) > 0;
}

const BlackBox::IndexSchema* BlackBox::getSchema(const std::string& name) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(name);
    if (it == indexes_.end()) return nullptr;
    return &it->second.schema;
}

const std::unordered_map<std::string, std::unordered_map<BlackBox::DocId, double>>* BlackBox::getNumericValues(const std::string& name) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(name);
    if (it == indexes_.end()) return nullptr;
    return &it->second.numericValues;
}

const std::unordered_map<std::string, std::unordered_map<BlackBox::DocId, bool>>* BlackBox::getBoolValues(const std::string& name) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(name);
    if (it == indexes_.end()) return nullptr;
    return &it->second.boolValues;
}

const std::unordered_map<std::string, std::unordered_map<std::string, std::vector<BlackBox::DocId>>>* BlackBox::getStringLists(const std::string& name) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(name);
    if (it == indexes_.end()) return nullptr;
    return &it->second.stringLists;
}

BlackBox::DocId BlackBox::indexDocument(const std::string& index, const std::string& jsonStr) {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) throw std::runtime_error("index not found");
    IndexState& idx = it->second;

    json j = json::parse(jsonStr);
    if (!validateDocument(idx, j)) {
        throw std::runtime_error("document does not conform to schema");
    }
    DocId id = applyUpsert(idx, 0, j, true);
    refreshAverages(idx);
    writeManifest();
    saveSnapshot(); // ensure durable .skd
    flushIfNeeded(index, idx);
    return id;
}

nlohmann::json BlackBox::getDocument(const std::string& index, DocId id) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) throw std::runtime_error("index not found");
    const auto& docs = it->second.documents;
    auto d = docs.find(id);
    if (d == docs.end()) throw std::runtime_error("Document ID not found");
    return d->second;
}

bool BlackBox::deleteDocument(const std::string& index, DocId id) {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return false;
    IndexState& idx = it->second;
    bool ok = applyDelete(idx, id, true);
    if (ok) {
        refreshAverages(idx);
        writeManifest();
        saveSnapshot(); // ensure durable .skd
        flushIfNeeded(index, idx);
    }
    return ok;
}

bool BlackBox::updateDocument(const std::string& index, DocId id, const std::string& jsonStr, bool partial) {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
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

    applyUpsert(idx, id, merged, true);
    refreshAverages(idx);
    writeManifest();
    saveSnapshot(); // ensure durable .skd
    flushIfNeeded(index, idx);
    return true;
}
std::size_t BlackBox::documentCount(const std::string& index) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return 0;
    return it->second.documents.size();
}

std::vector<BlackBox::SearchHit> BlackBox::search(const std::string& index, const std::string& query, const std::string& mode, size_t maxResults, int maxEditDistance) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    const IndexState& idx = it->second;
    auto terms = tokenize(query);
    if (terms.empty()) return {};
    algo::SearchContext ctx{idx.documents, idx.invertedIndex, idx.docLengths, idx.avgDocLen, &idx.skipPointers};
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
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    const IndexState& idx = it->second;
    auto terms = tokenize(query);
    if (terms.empty()) return {};
    algo::SearchContext ctx{idx.documents, idx.invertedIndex, idx.docLengths, idx.avgDocLen, &idx.skipPointers};
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
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    auto it = indexes_.find(index);
    if (it == indexes_.end()) return {};
    IndexState& idx = const_cast<IndexState&>(it->second);
    if (idx.schema.vectorDim == 0 || queryVec.size() != idx.schema.vectorDim) return {};
    if (idx.annDirty) rebuildAnn(idx);

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
    auto scoreDoc = [&](uint32_t docId, const std::vector<float>& vec) {
        if (vec.size() != idx.schema.vectorDim) return;
        double score = dot(queryVec, vec) / (qn * norm(vec) + 1e-9);
        hits.push_back({docId, score});
    };

    // ANN: probe closest centroids then brute within buckets
    if (!idx.annCentroids.empty() && !idx.annBuckets.empty()) {
        double best1 = -1e9, best2 = -1e9;
        size_t c1 = 0, c2 = 0;
        for (size_t c = 0; c < idx.annCentroids.size(); ++c) {
            double s = dot(idx.annCentroids[c], queryVec);
            if (s > best1) { best2 = best1; c2 = c1; best1 = s; c1 = c; }
            else if (s > best2) { best2 = s; c2 = c; }
        }
        std::unordered_set<uint32_t> visited;
        auto probe = [&](size_t bucketIdx) {
            if (bucketIdx >= idx.annBuckets.size()) return;
            for (auto docId : idx.annBuckets[bucketIdx]) {
                if (!visited.insert(docId).second) continue;
                auto itv = idx.vectors.find(docId);
                if (itv == idx.vectors.end()) continue;
                scoreDoc(docId, itv->second);
            }
        };
        probe(c1);
        if (idx.annBuckets.size() > 1) probe(c2);
        // fallback to ensure some results
        if (hits.empty()) {
            for (const auto& kv : idx.vectors) scoreDoc(kv.first, kv.second);
        }
    } else {
        for (const auto& kv : idx.vectors) scoreDoc(kv.first, kv.second);
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

std::vector<BlackBox::IndexStats> BlackBox::stats() const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    std::vector<IndexStats> out;
    out.reserve(indexes_.size());
    for (const auto& kv : indexes_) {
        const auto& st = kv.second;
        IndexStats s;
        s.name = kv.first;
        s.documents = st.documents.size();
        s.segments = st.segments.size();
        s.vectors = st.vectors.size();
        s.annClusters = st.annClusters;
        s.walBytes = st.wal.offset;
        s.pendingOps = st.opsSinceFlush;
        s.avgDocLen = st.avgDocLen;
        out.push_back(std::move(s));
    }
    return out;
}

nlohmann::json BlackBox::config() const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    json j{
        {"data_dir", dataDir_},
        {"flush_every_docs", flushEveryDocs_},
        {"merge_segments_at", mergeSegmentsAt_},
        {"compress_snapshots", compressSnapshots_},
        {"default_ann_clusters", defaultAnnClusters_}
    };
    return j;
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

void BlackBox::rebuildSkipPointers(IndexState& idx) {
    constexpr size_t kSkipStride = 8;
    idx.skipPointers.clear();
    idx.skipPointers.reserve(idx.invertedIndex.size());
    for (const auto& kv : idx.invertedIndex) {
        const auto& plist = kv.second;
        std::vector<algo::SkipEntry> skips;
        if (!plist.empty()) {
            for (size_t i = 0; i < plist.size(); i += kSkipStride) {
                skips.push_back({static_cast<uint32_t>(i), plist[i].id});
            }
        }
        idx.skipPointers.emplace(kv.first, std::move(skips));
    }
}

void BlackBox::rebuildAnn(IndexState& idx) const {
    const uint32_t dim = idx.schema.vectorDim;
    if (dim == 0) { idx.annCentroids.clear(); idx.annBuckets.clear(); idx.annDirty = false; return; }
    if (idx.vectors.empty()) { idx.annCentroids.clear(); idx.annBuckets.clear(); idx.annDirty = false; return; }

    const size_t k = std::max<size_t>(1, std::min<size_t>(idx.annClusters, idx.vectors.size()));

    auto normalize = [](std::vector<float> v) {
        double n = 0.0;
        for (float x : v) n += static_cast<double>(x) * static_cast<double>(x);
        n = std::sqrt(n);
        if (n == 0) return v;
        for (auto& x : v) x = static_cast<float>(x / n);
        return v;
    };
    auto cosine = [](const std::vector<float>& a, const std::vector<float>& b) {
        double s = 0.0;
        size_t m = std::min(a.size(), b.size());
        for (size_t i = 0; i < m; ++i) s += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        return s;
    };

    // Seed centroids using first k vectors
    idx.annCentroids.clear();
    idx.annCentroids.reserve(k);
    size_t seeded = 0;
    for (const auto& kv : idx.vectors) {
        if (kv.second.size() != dim) continue;
        idx.annCentroids.push_back(normalize(kv.second));
        if (++seeded >= k) break;
    }
    if (idx.annCentroids.empty()) { idx.annDirty = false; return; }

    // Lloyd-style refinement (small iterations)
    constexpr int iters = 2;
    for (int iter = 0; iter < iters; ++iter) {
        std::vector<std::vector<float>> newC(idx.annCentroids.size(), std::vector<float>(dim, 0.0f));
        std::vector<uint32_t> counts(idx.annCentroids.size(), 0);
        for (const auto& kv : idx.vectors) {
            if (kv.second.size() != dim) continue;
            auto v = normalize(kv.second);
            double best = -1e9;
            size_t bestIdx = 0;
            for (size_t c = 0; c < idx.annCentroids.size(); ++c) {
                double score = cosine(v, idx.annCentroids[c]);
                if (score > best) { best = score; bestIdx = c; }
            }
            ++counts[bestIdx];
            for (size_t d = 0; d < dim; ++d) newC[bestIdx][d] += v[d];
        }
        for (size_t c = 0; c < idx.annCentroids.size(); ++c) {
            if (counts[c] == 0) continue;
            for (size_t d = 0; d < dim; ++d) newC[c][d] = static_cast<float>(newC[c][d] / counts[c]);
            idx.annCentroids[c] = normalize(newC[c]);
        }
    }

    idx.annBuckets.assign(idx.annCentroids.size(), {});
    for (const auto& kv : idx.vectors) {
        if (kv.second.size() != dim) continue;
        auto v = normalize(kv.second);
        double best = -1e9;
        size_t bestIdx = 0;
        for (size_t c = 0; c < idx.annCentroids.size(); ++c) {
            double score = cosine(v, idx.annCentroids[c]);
            if (score > best) { best = score; bestIdx = c; }
        }
        idx.annBuckets[bestIdx].push_back(kv.first);
    }
    idx.annDirty = false;
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

BlackBox::DocId BlackBox::applyUpsert(IndexState& idx, DocId id, const nlohmann::json& doc, bool logWal) {
    DocId assignId = id == 0 ? idx.nextId++ : id;

    // remove old if updating
    auto existing = idx.documents.find(assignId);
    if (existing != idx.documents.end()) {
        removeJson(idx, assignId, existing->second);
        idx.docLengths.erase(assignId);
        idx.vectors.erase(assignId);
        for (auto& kv : idx.boolValues) kv.second.erase(assignId);
        for (auto& kv : idx.numericValues) kv.second.erase(assignId);
        for (auto& kv : idx.stringLists) {
            for (auto& bucket : kv.second) {
                auto& vec = bucket.second;
                vec.erase(std::remove(vec.begin(), vec.end(), assignId), vec.end());
            }
        }
    }

    // vector extraction
    if (!idx.schema.vectorField.empty() && idx.schema.vectorDim > 0) {
        auto vf = idx.schema.vectorField;
        if (doc.contains(vf)) {
            const auto& arr = doc[vf];
            std::vector<float> vec;
            if (arr.is_array()) {
                for (size_t i = 0; i < idx.schema.vectorDim && i < arr.size(); ++i) {
                    vec.push_back(static_cast<float>(arr[i].get<double>()));
                }
                if (vec.size() == idx.schema.vectorDim) {
                    idx.vectors[assignId] = vec;
                }
            }
        }
    }

    idx.documents[assignId] = doc;
    idx.docLengths[assignId] = 0;
    indexStructured(idx, assignId, doc);

    if (logWal) {
        if (!idx.wal.stream.is_open() && !idx.wal.path.empty()) {
            idx.wal.open();
        }
        if (idx.wal.stream.is_open()) {
            std::vector<uint8_t> cbor = json::to_cbor(doc);
            WalRecord rec;
            rec.op = WalOp::Upsert;
            rec.docId = assignId;
            rec.payload.assign(reinterpret_cast<char*>(cbor.data()), reinterpret_cast<char*>(cbor.data()) + cbor.size());
            if (!idx.wal.append(rec)) {
                std::cerr << "WAL append failed for " << idx.wal.path << "\n";
            }
        }
        else {
            std::cerr << "WAL not open for path " << idx.wal.path << "\n";
        }
    }
    rebuildSkipPointers(idx);
    idx.annDirty = true;
    ++idx.opsSinceFlush;
    return assignId;
}

bool BlackBox::applyDelete(IndexState& idx, DocId id, bool logWal) {
    auto it = idx.documents.find(id);
    if (it == idx.documents.end()) return false;
    removeJson(idx, id, it->second);
    idx.documents.erase(it);
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
    if (logWal) {
        if (!idx.wal.stream.is_open() && !idx.wal.path.empty()) {
            idx.wal.open();
        }
        if (idx.wal.stream.is_open()) {
            WalRecord rec;
            rec.op = WalOp::Delete;
            rec.docId = id;
            if (!idx.wal.append(rec)) {
                std::cerr << "WAL append failed for " << idx.wal.path << "\n";
            }
        }
        else {
            std::cerr << "WAL not open for path " << idx.wal.path << "\n";
        }
    }
    rebuildSkipPointers(idx);
    idx.annDirty = true;
    ++idx.opsSinceFlush;
    return true;
}

void BlackBox::flushIfNeeded(const std::string& index, IndexState& idx) {
    if (flushEveryDocs_ == 0) return;
    if (idx.opsSinceFlush < flushEveryDocs_) return;
    // build a new segment for the index and write manifest
    namespace fs = std::filesystem;
    if (dataDir_.empty()) return;

    // build segment chunk from current state
    SnapshotChunk chunk;
    chunk.vectorDim = idx.schema.vectorDim;
    chunk.docs.insert(chunk.docs.end(), idx.documents.begin(), idx.documents.end());
    chunk.docLens = idx.docLengths;
    chunk.index = idx.invertedIndex;
    chunk.vectors = idx.vectors;
    chunk.docValues = json{
        {"numeric", idx.numericValues},
        {"bool", idx.boolValues},
        {"strings", idx.stringLists}
    };

    double avg = idx.docLengths.empty() ? 0.0 : [&]() {
        uint64_t total = 0;
        for (const auto& kv : idx.docLengths) total += kv.second;
        return static_cast<double>(total) / static_cast<double>(idx.docLengths.size());
    }();

    fs::path segFile = fs::path(dataDir_) / (index + "_seg" + std::to_string(idx.segments.size()) + ".skd");
    if (writeSnapshotFile(segFile.string(), chunk, idx.nextId, avg, compressSnapshots_)) {
        SegmentMetadata meta;
        if (!idx.documents.empty()) {
            auto minmax = std::minmax_element(idx.documents.begin(), idx.documents.end(),
                [](const auto& a, const auto& b){ return a.first < b.first; });
            meta.minId = minmax.first->first;
            meta.maxId = minmax.second->first;
        }
        meta.file = segFile.filename().string();
        meta.walPos = idx.wal.offset;
        idx.segments.push_back(meta);
        // reset WAL
        idx.wal.reset();
        idx.wal.open();
        idx.opsSinceFlush = 0;
        writeManifest();
        maybeMergeSegments(index, idx);
    }
}

void BlackBox::maybeMergeSegments(const std::string& index, IndexState& idx) {
    if (mergeSegmentsAt_ == 0) return;
    if (idx.segments.size() < mergeSegmentsAt_) return;
    namespace fs = std::filesystem;
    SnapshotChunk chunk;
    chunk.vectorDim = idx.schema.vectorDim;
    chunk.docs.insert(chunk.docs.end(), idx.documents.begin(), idx.documents.end());
    chunk.docLens = idx.docLengths;
    chunk.index = idx.invertedIndex;
    chunk.vectors = idx.vectors;
    chunk.docValues = json{
        {"numeric", idx.numericValues},
        {"bool", idx.boolValues},
        {"strings", idx.stringLists}
    };
    double avg = idx.docLengths.empty() ? 0.0 : [&]() {
        uint64_t total = 0;
        for (const auto& kv : idx.docLengths) total += kv.second;
        return static_cast<double>(total) / static_cast<double>(idx.docLengths.size());
    }();

    fs::path segFile = fs::path(dataDir_) / (index + "_merge" + std::to_string(idx.segments.size()) + ".skd");
    if (!writeSnapshotFile(segFile.string(), chunk, idx.nextId, avg, compressSnapshots_)) {
        std::cerr << "BlackBox: mergeSegments failed to write " << segFile << "\n";
        return;
    }
    // delete old segment files
    for (const auto& seg : idx.segments) {
        fs::path p = fs::path(dataDir_) / seg.file;
        std::error_code ec;
        fs::remove(p, ec);
    }
    idx.segments.clear();
    SegmentMetadata meta;
    if (!idx.documents.empty()) {
        auto minmax = std::minmax_element(idx.documents.begin(), idx.documents.end(),
            [](const auto& a, const auto& b){ return a.first < b.first; });
        meta.minId = minmax.first->first;
        meta.maxId = minmax.second->first;
    }
    meta.file = segFile.filename().string();
    meta.walPos = 0;
    idx.segments.push_back(meta);
    idx.wal.reset();
    idx.wal.open();
    idx.opsSinceFlush = 0;
    writeManifest();
}

void BlackBox::writeManifest() const {
    namespace fs = std::filesystem;
    if (dataDir_.empty()) return;
    fs::path manifestPath = fs::path(dataDir_) / "index.manifest";
    json manifest = {{"version", 1}, {"indexes", json::array()}};
    for (const auto& entry : indexes_) {
        json segs = json::array();
        for (const auto& seg : entry.second.segments) {
            segs.push_back({{"file", seg.file}, {"min_id", seg.minId}, {"max_id", seg.maxId}, {"wal_pos", seg.walPos}});
        }
        manifest["indexes"].push_back({
            {"name", entry.first},
            {"segments", segs},
            {"schema", entry.second.schema.schema},
            {"ann_clusters", entry.second.annClusters}
        });
    }
    std::ofstream out(manifestPath, std::ios::binary | std::ios::trunc);
    out << manifest.dump(2);
}

void BlackBox::replayWal(IndexState& idx) {
    if (!idx.wal.stream.is_open()) return;
    auto records = readWalRecords(idx.wal.path);
    for (const auto& rec : records) {
        if (rec.op == WalOp::Upsert) {
            auto j = json::from_cbor(rec.payload, nullptr, false);
            if (j.is_discarded()) continue;
            if (!validateDocument(idx, j)) continue;
            idx.nextId = std::max<DocId>(idx.nextId, rec.docId + 1);
            applyUpsert(idx, rec.docId, j, false);
        } else if (rec.op == WalOp::Delete) {
            applyDelete(idx, rec.docId, false);
        }
    }
}

bool BlackBox::saveSnapshot(const std::string& path) const {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
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
            if (!writeSnapshotFile(shardFile.string(), chunk, tmp.nextId, avg, compressSnapshots_)) {
                return false;
            }
            uint32_t segMin = ids[start];
            uint32_t segMax = ids[end - 1];
            segs.push_back({
                {"file", shardFile.filename().string()},
                {"min_id", segMin},
                {"max_id", segMax},
                {"wal_pos", 0}
            });
        }
        manifest["indexes"].push_back({
            {"name", name},
            {"segments", segs},
            {"schema", idx.schema.schema},
            {"ann_clusters", idx.annClusters}
        });
    }
    std::ofstream out(manifestPath, std::ios::binary | std::ios::trunc);
    out << manifest.dump(2);
    return static_cast<bool>(out);
}

bool BlackBox::loadSnapshot(const std::string& path) {
    std::lock_guard<std::recursive_mutex> lk(mutex_);
    namespace fs = std::filesystem;
    fs::path manifestPath = path.empty() ? fs::path(dataDir_) / "index.manifest" : fs::path(path);
    if (!fs::exists(manifestPath)) {
        std::cerr << "BlackBox: manifest not found at " << manifestPath << "\n";
        return false;
    }
    std::ifstream in(manifestPath);
    if (!in) {
        std::cerr << "BlackBox: failed to open manifest " << manifestPath << "\n";
        return false;
    }
    json manifest = json::parse(in, nullptr, false);
    if (manifest.is_discarded()) {
        std::cerr << "BlackBox: manifest parse error\n";
        return false;
    }
    auto arr = manifest.value("indexes", json::array());
    if (!arr.is_array()) return false;

    indexes_.clear();

    for (const auto& idxJson : arr) {
        if (!idxJson.is_object()) continue;
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
        state.annClusters = idxJson.value("ann_clusters", defaultAnnClusters_);
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
            std::string file;
            uint32_t minId = 0;
            uint32_t maxId = 0;
            uint64_t walPos = 0;
            if (seg.is_string()) {
                file = seg.get<std::string>();
            } else if (seg.is_object()) {
                file = seg.value("file", "");
                minId = seg.value("min_id", 0u);
                maxId = seg.value("max_id", 0u);
                walPos = seg.value("wal_pos", 0ull);
            } else {
                continue;
            }
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
                        if (!itn->is_object()) continue;
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
                        if (!itb->is_object()) continue;
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
                        if (!its->is_object()) continue;
                        for (auto itb = its->begin(); itb != its->end(); ++itb) {
                            if (!itb.value().is_array()) continue;
                            std::vector<DocId> idsArr;
                            for (const auto& val : itb.value()) idsArr.push_back(val.get<uint32_t>());
                            state.stringLists[its.key()][itb.key()] = std::move(idsArr);
                        }
                    }
                }
            }
            // Record segments metadata from manifest (fall back to deriving id range if absent)
            if ((minId == 0 || maxId == 0) && !chunk.docs.empty()) {
                uint32_t derivedMin = std::numeric_limits<uint32_t>::max();
                uint32_t derivedMax = 0;
                for (const auto& kv : chunk.docs) {
                    derivedMin = std::min(derivedMin, kv.first);
                    derivedMax = std::max(derivedMax, kv.first);
                }
                if (minId == 0) minId = derivedMin;
                if (maxId == 0) maxId = derivedMax;
            }
            SegmentMetadata meta;
            meta.file = file;
            meta.minId = minId;
            meta.maxId = maxId;
            meta.walPos = walPos;
            state.segments.push_back(meta);
        }

        // Ensure postings are sorted/unique
        for (auto& termEntry : state.invertedIndex) {
            auto& vec = termEntry.second;
            std::sort(vec.begin(), vec.end(), [](const algo::Posting& a, const algo::Posting& b) { return a.id < b.id; });
            vec.erase(std::unique(vec.begin(), vec.end(), [](const algo::Posting& a, const algo::Posting& b){return a.id==b.id;}), vec.end());
        }
        rebuildSkipPointers(state);
        state.annDirty = !state.vectors.empty();

        refreshAverages(state);
        // init WAL
        if (!dataDir_.empty()) {
            state.wal.path = (fs::path(dataDir_) / (name + ".wal")).string();
            state.wal.open();
            replayWal(state);
        }
        indexes_[name] = std::move(state);
        std::cerr << "BlackBox: loaded index " << name << " segments=" << segments.size() << "\n";
    }
    return true;
}

void BlackBox::loadWalOnly() {
    namespace fs = std::filesystem;
    if (dataDir_.empty()) return;
    std::cerr << "BlackBox: scanning WAL-only in " << dataDir_ << "\n";
    for (const auto& entry : fs::directory_iterator(dataDir_)) {
        if (!entry.is_regular_file()) continue;
        auto path = entry.path();
        if (path.extension() != ".wal") continue;
        std::string name = path.stem().string();
        if (indexes_.count(name)) continue;
        IndexState state;
        state.annClusters = defaultAnnClusters_;
        state.wal.path = path.string();
        state.wal.open();
        replayWal(state);
        indexes_[name] = std::move(state);
        std::cerr << "BlackBox: built index " << name << " from WAL\n";
    }
    writeManifest();
}

} // namespace minielastic
