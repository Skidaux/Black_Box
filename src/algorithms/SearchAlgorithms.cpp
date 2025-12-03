#include "minielastic/algorithms/SearchAlgorithms.hpp"

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <limits>
#include <numeric>

namespace minielastic::algo {

namespace {
constexpr size_t kSkipStride = 8;

// Advance position inside a postings list using skip pointers (if available) to at least target docId.
inline void advanceWithSkip(const std::vector<Posting>& plist,
                            const std::vector<SkipEntry>* skips,
                            size_t& pos,
                            uint32_t target) {
    if (pos >= plist.size()) return;
    if (!skips || skips->empty()) {
        while (pos < plist.size() && plist[pos].id < target) ++pos;
        return;
    }
    // Jump by stride blocks while docId < target
    size_t block = pos / kSkipStride;
    while (block < skips->size() && plist[pos].id < target) {
        const auto& s = (*skips)[block];
        if (s.docId < target && s.pos > pos) {
            pos = s.pos;
            block = pos / kSkipStride;
        } else {
            break;
        }
    }
    while (pos < plist.size() && plist[pos].id < target) ++pos;
}

// Intersect all posting lists (AND semantics) using skip pointers; returns doc IDs present in all lists.
std::vector<uint32_t> intersectAll(const SearchContext& ctx, const std::vector<std::string>& terms) {
    if (terms.empty()) return {};
    // gather posting pointers
    std::vector<const std::vector<Posting>*> lists;
    std::vector<const std::vector<SkipEntry>*> skips;
    lists.reserve(terms.size());
    skips.reserve(terms.size());
    for (const auto& t : terms) {
        auto it = ctx.index.find(t);
        if (it == ctx.index.end() || it->second.empty()) return {};
        lists.push_back(&it->second);
        if (ctx.skips) {
            auto skIt = ctx.skips->find(t);
            skips.push_back(skIt != ctx.skips->end() ? &skIt->second : nullptr);
        } else {
            skips.push_back(nullptr);
        }
    }
    // sort by list size to reduce work
    std::vector<size_t> order(lists.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return lists[a]->size() < lists[b]->size();
    });

    // positions per list
    std::vector<size_t> pos(lists.size(), 0);
    std::vector<uint32_t> out;
    // initialize target as smallest list first element
    uint32_t target = (*lists[order[0]])[0].id;

    while (true) {
        bool anyEnd = false;
        uint32_t maxDoc = target;
        // align all lists to target (or beyond)
        for (size_t k = 0; k < order.size(); ++k) {
            size_t idx = order[k];
            advanceWithSkip(*lists[idx], skips[idx], pos[idx], target);
            if (pos[idx] >= lists[idx]->size()) { anyEnd = true; break; }
            uint32_t cur = (*lists[idx])[pos[idx]].id;
            if (cur > maxDoc) maxDoc = cur;
        }
        if (anyEnd) break;
        bool allEqual = true;
        for (size_t k = 0; k < order.size(); ++k) {
            size_t idx = order[k];
            if ((*lists[idx])[pos[idx]].id != maxDoc) {
                allEqual = false;
                break;
            }
        }
        if (allEqual) {
            out.push_back(maxDoc);
            // move all forward
            for (size_t k = 0; k < order.size(); ++k) {
                ++pos[order[k]];
            }
            if (pos[order[0]] >= lists[order[0]]->size()) break;
            target = (*lists[order[0]])[pos[order[0]]].id;
        } else {
            target = maxDoc;
        }
    }
    return out;
}

} // namespace

std::vector<SearchHit> searchLexical(const SearchContext& ctx, const std::vector<std::string>& terms) {
    if (terms.empty()) return {};

    auto ids = intersectAll(ctx, terms);
    std::vector<SearchHit> hits;
    hits.reserve(ids.size());
    for (auto id : ids) {
        double score = 0.0;
        for (const auto& term : terms) {
            auto it = ctx.index.find(term);
            if (it == ctx.index.end()) continue;
            const auto& plist = it->second;
            auto itPos = std::lower_bound(plist.begin(), plist.end(), id, [](const Posting& p, uint32_t v){ return p.id < v; });
            if (itPos != plist.end() && itPos->id == id) score += itPos->tf;
        }
        hits.push_back({id, score});
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

    auto ids = intersectAll(ctx, terms);
    if (ids.empty()) return {};

    for (const auto& term : terms) {
        auto it = ctx.index.find(term);
        if (it == ctx.index.end()) return {};
        const auto& plist = it->second;
        const double df = static_cast<double>(plist.size());
        const double idf = std::log((N - df + 0.5) / (df + 0.5) + 1.0);
        for (auto id : ids) {
            auto itPos = std::lower_bound(plist.begin(), plist.end(), id, [](const Posting& p, uint32_t v){ return p.id < v; });
            if (itPos == plist.end() || itPos->id != id) continue;
            const double tf = static_cast<double>(itPos->tf);
            auto dlIt = ctx.docLengths.find(id);
            const double dl = dlIt != ctx.docLengths.end() ? static_cast<double>(dlIt->second) : 1.0;
            const double denom = tf + k1 * (1.0 - b + b * (dl / avgLen));
            const double score = idf * (tf * (k1 + 1.0)) / denom;
            scores[id] += score;
        }
    }

    std::vector<SearchHit> hits;
    hits.reserve(scores.size());
    for (const auto& kv : scores) hits.push_back({kv.first, kv.second});
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
