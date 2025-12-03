// Benchmark + capability test script for the BlackBox HTTP API.
// Usage: node scripts/test_client.js
// Ensure the server is running at http://127.0.0.1:8080

const fs = require("fs");
const path = require("path");
const axios = require("axios").create({
  baseURL: "http://127.0.0.1:8080",
  timeout: 10000,
  validateStatus: () => true,
});

const hrMs = () => Number(process.hrtime.bigint()) / 1e6;

async function timeStep(label, fn) {
  const start = hrMs();
  const res = await fn();
  const end = hrMs();
  return { label, ms: end - start, result: res };
}

async function main() {
  const indexName = "demo";
  const benchmark = {
    timestamp: new Date().toISOString(),
    baseURL: axios.defaults.baseURL,
    index: indexName,
    docsIndexed: 0,
    timings: [],
    queries: [],
    notes: "Measures create/index/update/search latencies. Vector search uses ANN+fallback.",
  };

  try {
    // 1) Create index with schema (text, arrays, bool, number, vector)
    const createStep = await timeStep("create_index", async () =>
      axios.post("/v1/indexes", {
        name: indexName,
        schema: {
          fields: {
            title: "text",
            body: "text",
            tags: "array",
            labels: "array",
            flag: "bool",
            priority: "number",
            vec: { type: "vector", dim: 3 },
          },
        },
      })
    );
    benchmark.timings.push({
      step: createStep.label,
      ms: createStep.ms,
      status: createStep.result.status,
    });
    console.log("create index:", createStep.result.status, createStep.result.data);

    // 2) Index sample documents
    const docs = [
      { title: "Doc1", body: "quick brown fox jumps over the lazy dog", tags: ["animal", "fast"], labels: ["short"], flag: true, priority: 5, vec: [1, 0, 0] },
      { title: "Doc2", body: "fast red fox leaped high", tags: ["animal"], labels: ["medium"], flag: false, priority: 3, vec: [0.9, 0.1, 0] },
      { title: "Doc3", body: "sleepy dog rests quietly", tags: ["animal", "sleep"], labels: ["short"], flag: true, priority: 1, vec: [0, 1, 0] },
    ];

    const docTimings = [];
    for (const doc of docs) {
      const step = await timeStep(`index_${doc.title}`, async () =>
        axios.post(`/v1/${indexName}/doc`, doc, { headers: { "Content-Type": "application/json" } })
      );
      docTimings.push({ doc: doc.title, ms: step.ms, status: step.result.status });
      console.log("index doc:", step.result.status, step.result.data);
    }
    benchmark.timings.push({
      step: "index_documents_total",
      ms: docTimings.reduce((s, t) => s + t.ms, 0),
      status: "aggregate",
      perDoc: docTimings,
    });
    benchmark.docsIndexed = docs.length;

    // 3) Partial update
    const patchStep = await timeStep("patch_doc2", async () =>
      axios.patch(`/v1/${indexName}/doc/2`, { flag: true, priority: 4 }, { headers: { "Content-Type": "application/json" } })
    );
    benchmark.timings.push({
      step: patchStep.label,
      ms: patchStep.ms,
      status: patchStep.result.status,
    });
    console.log("patch doc2:", patchStep.result.status, patchStep.result.data);

    // 4) Searches (bm25, lexical, fuzzy, semantic, hybrid, filtered, vector)
    const queries = [
      { name: "bm25", params: { q: "quick fox", mode: "bm25" } },
      { name: "lexical", params: { q: "quick fox", mode: "lexical" } },
      { name: "fuzzy", params: { q: "quik fox", mode: "fuzzy", distance: 2 } },
      { name: "semantic", params: { q: "quick brown fox", mode: "semantic" } },
      { name: "hybrid", params: { q: "quick fox", mode: "hybrid", w_bm25: 1, w_semantic: 1, w_lexical: 0.5 } },
      { name: "bm25_tag", params: { q: "fox", mode: "bm25", tag: "animal" } },
      { name: "bm25_label_flag", params: { q: "fox", mode: "bm25", label: "short", flag: "true" } },
      { name: "bm25_priority_range", params: { q: "fox", mode: "bm25", filter_priority_min: 3 } },
      // vector search: include q placeholder to satisfy server validation
      { name: "vector", params: { q: "vector", mode: "vector", vec: "1,0,0" } },
    ];

    for (const q of queries) {
      const qs = new URLSearchParams(q.params).toString();
      const step = await timeStep(`search_${q.name}`, async () =>
        axios.get(`/v1/${indexName}/search?${qs}`)
      );
      const data = step.result.data || {};
      const hits = data.data && data.data.hits ? data.data.hits.length : 0;
      benchmark.queries.push({
        name: q.name,
        params: q.params,
        ms: step.ms,
        status: step.result.status,
        hits,
        total: data.data ? data.data.total : undefined,
      });
      console.log(`search (${q.name}):`, step.result.status, JSON.stringify(data, null, 2));
    }

    // 5) Persist benchmark JSON
    const outPath = path.join(__dirname, "benchmark_results.json");
    fs.writeFileSync(outPath, JSON.stringify(benchmark, null, 2), "utf8");
    console.log(`Benchmark results written to ${outPath}`);
  } catch (err) {
    console.error("Test failed:", err.message);
  }
}

main();
