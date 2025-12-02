// Basic standalone test script for the BlackBox HTTP API using axios.
// Usage: node scripts/test_client.js
// Ensure the server is running at http://127.0.0.1:8080

const axios = require("axios").create({
  baseURL: "http://127.0.0.1:8080",
  timeout: 5000,
  validateStatus: () => true,
});

async function main() {
  try {
    const indexName = "demo";

    // 1) Create an index with schema (including vector)
    let resp = await axios.post("/v1/indexes", {
      name: indexName,
      schema: {
        fields: {
          title: "text",
          body: "text",
          tags: "array",
          labels: "array",
          flag: "bool",
          priority: "number",
          vec: { type: "vector", dim: 3 }
        },
      },
    });
    console.log("create index:", resp.status, resp.data);

    // 2) Index a few docs with tags/labels/flag
    const docs = [
      { title: "Doc1", body: "quick brown fox jumps over the lazy dog", tags: ["animal", "fast"], labels: ["short"], flag: true, priority: 5, vec: [1, 0, 0] },
      { title: "Doc2", body: "fast red fox leaped high", tags: ["animal"], labels: ["medium"], flag: false, priority: 3, vec: [0.9, 0.1, 0] },
      { title: "Doc3", body: "sleepy dog rests quietly", tags: ["animal", "sleep"], labels: ["short"], flag: true, priority: 1, vec: [0, 1, 0] },
    ];

    for (const doc of docs) {
      resp = await axios.post(`/v1/${indexName}/doc`, doc, {
        headers: { "Content-Type": "application/json" },
      });
      console.log("index doc:", resp.status, resp.data);
    }

    // 3) Update a document (PATCH partial)
    resp = await axios.patch(`/v1/${indexName}/doc/2`, { flag: true, priority: 4 }, { headers: { "Content-Type": "application/json" } });
    console.log("patch doc2:", resp.status, resp.data);

    // 4) Run searches with different modes and filters, plus vector and numeric filters
    const queries = [
      { q: "quick fox", mode: "bm25" },
      { q: "quick fox", mode: "lexical" },
      { q: "quik fox", mode: "fuzzy", distance: 2 },
      { q: "quick brown fox", mode: "semantic" },
      { q: "quick fox", mode: "hybrid", w_bm25: 1, w_semantic: 1, w_lexical: 0.5 },
      { q: "fox", mode: "bm25", tag: "animal" },
      { q: "fox", mode: "bm25", label: "short", flag: "true" },
      { q: "fox", mode: "bm25", filter_priority_min: 3 },
      // vector search: mode=vector, vec=comma-separated values (include q to satisfy servers requiring it)
      { q: "vector", mode: "vector", vec: "1,0,0" },
    ];

    for (const params of queries) {
      const qs = new URLSearchParams(params).toString();
      resp = await axios.get(`/v1/${indexName}/search?${qs}`);
      console.log(`search (${params.mode}${params.tag ? " tag=" + params.tag : ""}${params.label ? " label=" + params.label : ""}${params.flag ? " flag=" + params.flag : ""}):`,
        resp.status, JSON.stringify(resp.data, null, 2));
    }
  } catch (err) {
    console.error("Test failed:", err.message);
  }
}

main();
