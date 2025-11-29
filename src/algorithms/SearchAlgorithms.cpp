#include "minielastic/algorithms/SearchAlgorithms.hpp"

#include <algorithm>
#include <cmath>
#include <unordered_map>

namespace minielastic::algo {

std::vector<SearchHit> searchLexical(const SearchContext& ctx, const std::vector<std::string>& terms) {
    if (terms.empty()) return {};

    std::unordered_map<uint32_t, uint32_t> termHits;
    std::unordered_map<uint32_t, double> scores;

    for (const auto& term : terms) {
        auto it = ctx.index.find(term);
        if (it == ctx.index.end()) {
            return {}; // AND semantics: missing term aborts
        }
        for (const auto& p : it->second) {
            termHits[p.id] += 1;
            scores[p.id] += p.tf;
        }
    }

    std::vector<SearchHit> hits;
    for (const auto& kv : termHits) {
        if (kv.second == terms.size()) {
            hits.push_back({kv.first, scores[kv.first]});
        }
    }

    std::sort(hits.begin(), hits.end(), [](const SearchHit& a, const SearchHit& b) {
        if (a.score == b.score) return a.id < b.id;
        return a.score > b.score;
    });
    return hits;
}

std::vector<SearchHit> searchBm25(const SearchContext& ctx, const std::vector<std::string>& terms, size_t maxResults) {
    if (terms.empty()) return {};
    const double k1 = 1.5;
    const double b = 0.75;
    const double avgLen = ctx.avgDocLen > 0 ? ctx.avgDocLen : 1.0;
    const double N = static_cast<double>(ctx.docs.size());

    std::unordered_map<uint32_t, double> scores;
    std::unordered_map<uint32_t, uint32_t> termHits;

    for (const auto& term : terms) {
        auto it = ctx.index.find(term);
        if (it == ctx.index.end()) {
            return {}; // AND semantics
        }
        const auto& plist = it->second;
        const double df = static_cast<double>(plist.size());
        const double idf = std::log((N - df + 0.5) / (df + 0.5) + 1.0);

        for (const auto& p : plist) {
            const double tf = static_cast<double>(p.tf);
            auto dlIt = ctx.docLengths.find(p.id);
            const double dl = dlIt != ctx.docLengths.end() ? static_cast<double>(dlIt->second) : 1.0;
            const double denom = tf + k1 * (1.0 - b + b * (dl / avgLen));
            const double score = idf * (tf * (k1 + 1.0)) / denom;
            scores[p.id] += score;
            termHits[p.id] += 1;
        }
    }

    std::vector<SearchHit> hits;
    hits.reserve(scores.size());
    for (const auto& kv : scores) {
        if (termHits[kv.first] == terms.size()) {
            hits.push_back({kv.first, kv.second});
        }
    }
    std::sort(hits.begin(), hits.end(), [](const SearchHit& a, const SearchHit& b) {
        if (a.score == b.score) return a.id < b.id;
        return a.score > b.score;
    });
    if (hits.size() > maxResults) hits.resize(maxResults);
    return hits;
}

int editDistance(const std::string& a, const std::string& b, int maxCost) {
    const int n = static_cast<int>(a.size());
    const int m = static_cast<int>(b.size());
    if (std::abs(n - m) > maxCost) return maxCost + 1;

    std::vector<int> prev(m + 1), curr(m + 1);
    for (int j = 0; j <= m; ++j) prev[j] = j;

    for (int i = 1; i <= n; ++i) {
        curr[0] = i;
        int rowMin = curr[0];
        for (int j = 1; j <= m; ++j) {
            int cost = a[i - 1] == b[j - 1] ? 0 : 1;
            curr[j] = std::min({prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost});
            rowMin = std::min(rowMin, curr[j]);
        }
        if (rowMin > maxCost) return maxCost + 1;
        std::swap(prev, curr);
    }
    return prev[m];
}

std::vector<SearchHit> searchFuzzy(const SearchContext& ctx, const std::vector<std::string>& terms, int maxEditDistance, size_t maxResults) {
    if (terms.empty()) return {};
    std::vector<std::string> expandedTerms;
    expandedTerms.reserve(ctx.index.size());

    for (const auto& term : terms) {
        for (const auto& entry : ctx.index) {
            if (editDistance(term, entry.first, maxEditDistance) <= maxEditDistance) {
                expandedTerms.push_back(entry.first);
            }
        }
    }
    if (expandedTerms.empty()) return {};
    return searchBm25(ctx, expandedTerms, maxResults);
}

std::vector<SearchHit> searchSemantic(const SearchContext& ctx, const std::vector<std::string>& terms, size_t maxResults) {
    if (terms.empty()) return {};
    const double N = static_cast<double>(ctx.docs.size());
    if (N == 0) return {};

    std::unordered_map<std::string, uint32_t> qtf;
    for (const auto& t : terms) ++qtf[t];

    std::unordered_map<std::string, double> qWeights;
    double qNorm = 0.0;
    for (const auto& kv : qtf) {
        auto it = ctx.index.find(kv.first);
        if (it == ctx.index.end()) continue;
        double df = static_cast<double>(it->second.size());
        double idf = std::log((N - df + 0.5) / (df + 0.5) + 1.0);
        double w = static_cast<double>(kv.second) * idf;
        qWeights[kv.first] = w;
        qNorm += w * w;
    }
    if (qWeights.empty()) return {};
    qNorm = std::sqrt(qNorm);

    std::unordered_map<uint32_t, double> dot;
    std::unordered_map<uint32_t, double> docNormSq;

    for (const auto& kv : qWeights) {
        auto it = ctx.index.find(kv.first);
        if (it == ctx.index.end()) continue;
        double df = static_cast<double>(it->second.size());
        double idf = std::log((N - df + 0.5) / (df + 0.5) + 1.0);
        for (const auto& p : it->second) {
            double w = static_cast<double>(p.tf) * idf;
            dot[p.id] += kv.second * w;
            docNormSq[p.id] += w * w;
        }
    }

    std::vector<SearchHit> hits;
    hits.reserve(dot.size());
    for (const auto& kv : dot) {
        double denom = std::sqrt(docNormSq[kv.first]) * (qNorm > 0 ? qNorm : 1.0);
        if (denom == 0) continue;
        double score = kv.second / denom;
        hits.push_back({kv.first, score});
    }
    std::sort(hits.begin(), hits.end(), [](const SearchHit& a, const SearchHit& b) {
        if (a.score == b.score) return a.id < b.id;
        return a.score > b.score;
    });
    if (hits.size() > maxResults) hits.resize(maxResults);
    return hits;
}

} // namespace minielastic::algo
