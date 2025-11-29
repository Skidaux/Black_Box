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
    // 1) Create an index
    const indexName = "demo";
    let resp = await axios.post("/v1/indexes", {
      name: indexName,
      schema: { fields: { title: "text", body: "text" } },
    });
    console.log("create index:", resp.status, resp.data);

    // 2) Index a few docs
    const docs = [
      { title: "Doc1", body: "quick brown fox jumps over the lazy dog" },
      { title: "Doc2", body: "fast red fox leaped high" },
      { title: "Doc3", body: "sleepy dog rests quietly" },
    ];

    for (const doc of docs) {
      resp = await axios.post(`/v1/${indexName}/doc`, doc, {
        headers: { "Content-Type": "application/json" },
      });
      console.log("index doc:", resp.status, resp.data);
    }

    // 3) Run searches with different modes
    const queries = [
      { q: "quick fox", mode: "bm25" },
      { q: "quick fox", mode: "lexical" },
      { q: "quik fox", mode: "fuzzy", distance: 2 },
      { q: "quick brown fox", mode: "semantic" },
      { q: "quick fox", mode: "hybrid", w_bm25: 1, w_semantic: 1, w_lexical: 0.5 },
    ];

    for (const params of queries) {
      const qs = new URLSearchParams(params).toString();
      resp = await axios.get(`/v1/${indexName}/search?${qs}`);
      console.log(`search (${params.mode}):`, resp.status, JSON.stringify(resp.data, null, 2));
    }
  } catch (err) {
    console.error("Test failed:", err.message);
  }
}

main();
