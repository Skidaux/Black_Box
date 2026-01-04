#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>

namespace minielastic::algo {

struct Posting {
    uint32_t id;
    uint32_t tf;
};

struct SearchHit {
    uint32_t id;
    double score;
};

struct SkipEntry {
    uint32_t pos;    // index inside posting list
    uint32_t docId;  // docId at that position
};

struct SearchContext {
    const std::unordered_map<uint32_t, nlohmann::json>& docs;
    const std::unordered_map<std::string, std::vector<Posting>>& index;
    const std::unordered_map<uint32_t, uint32_t>& docLengths;
    double avgDocLen;
    const std::unordered_map<std::string, std::vector<SkipEntry>>* skips = nullptr;
};

std::vector<SearchHit> searchLexical(const SearchContext& ctx, const std::vector<std::string>& terms);
std::vector<SearchHit> searchBm25(const SearchContext& ctx, const std::vector<std::string>& terms, size_t maxResults);
std::vector<SearchHit> searchBm25Or(const SearchContext& ctx, const std::vector<std::string>& terms, size_t maxResults);
std::vector<SearchHit> searchFuzzy(const SearchContext& ctx, const std::vector<std::string>& terms, int maxEditDistance, size_t maxResults);
std::vector<SearchHit> searchSemantic(const SearchContext& ctx, const std::vector<std::string>& terms, size_t maxResults);

int editDistance(const std::string& a, const std::string& b, int maxCost);

} // namespace minielastic::algo
