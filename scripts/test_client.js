// Benchmark + capability test script for the BlackBox HTTP API.
// Usage: node scripts/test_client.js
// Ensure the server is running at http://127.0.0.1:8080

const fs = require("fs");
const path = require("path");
const axios = require("axios").create({
  baseURL: "http://127.0.0.1:8080",
  timeout: 100000,
  validateStatus: () => true,
});

const hrMs = () => Number(process.hrtime.bigint()) / 1e6;
const randChoice = (arr) => arr[Math.floor(Math.random() * arr.length)];
const randomText = (words, len) => {
  let out = [];
  for (let i = 0; i < len; ++i) out.push(randChoice(words));
  return out.join(" ");
};

const faviconPath = path.join(__dirname, "favicon.ico");
let faviconBase64 = null;
try {
  const buf = fs.readFileSync(faviconPath);
  faviconBase64 = buf.toString("base64");
} catch (err) {
  console.warn("Warning: failed to read favicon.ico:", err.message);
}

async function timeStep(label, fn) {
  const start = hrMs();
  const res = await fn();
  const end = hrMs();
  return { label, ms: end - start, result: res };
}

async function ensureIndex(name, schema) {
  const res = await axios.post("/v1/indexes", { name, schema });
  if (res.status !== 201 && res.status !== 400) {
    throw new Error(`Failed to create index ${name}: ${res.status}`);
  }
  return res;
}

async function indexDoc(index, doc) {
  return axios.post(`/v1/${index}/doc`, doc, {
    headers: { "Content-Type": "application/json" },
  });
}

async function fetchDoc(index, id) {
  return axios.get(`/v1/${index}/doc/${id}`);
}

async function patchDoc(index, id, body) {
  return axios.patch(`/v1/${index}/doc/${id}`, body, {
    headers: { "Content-Type": "application/json" },
  });
}

async function deleteDoc(index, id) {
  return axios.delete(`/v1/${index}/doc/${id}`);
}

async function runSearch(index, params) {
  const qs = new URLSearchParams(params).toString();
  return axios.get(`/v1/${index}/search?${qs}`);
}

async function main() {
  const primaryIndex = "demo";
  const relatedIndex = "related_demo";
  const benchmark = {
    timestamp: new Date().toISOString(),
    baseURL: axios.defaults.baseURL,
    indexes: [primaryIndex, relatedIndex],
    docsIndexed: {},
    timings: [],
    queries: [],
    notes:
      "Exercises multi-index usage, custom IDs, relations, binary images, custom aggregations, and relation-aware queries.",
  };
  benchmark.docsIndexed[primaryIndex] = 0;
  benchmark.docsIndexed[relatedIndex] = 0;

  try {
    // 1) Create indexes
    const primarySchema = {
      fields: {
        title: "text",
        body: "text",
        tags: "array",
        labels: "array",
        flag: "bool",
        priority: "number",
        vec: { type: "vector", dim: 3 },
      },
      relation: {
        field: "parent",
        target_index: relatedIndex,
      },
    };
    const relatedSchema = {
      fields: {
        title: "text",
        body: "text",
        sku: "text",
        favicon: { type: "image", max_kb: 64 },
      },
      doc_id: {
        field: "sku",
        type: "string",
      },
    };
    const createPrimary = await timeStep("create_primary", () =>
      ensureIndex(primaryIndex, primarySchema)
    );
    benchmark.timings.push({
      step: createPrimary.label,
      ms: createPrimary.ms,
      status: createPrimary.result.status,
    });
    console.log(
      "create primary index:",
      createPrimary.result.status,
      createPrimary.result.data
    );
    const createRelated = await timeStep("create_related", () =>
      ensureIndex(relatedIndex, relatedSchema)
    );
    benchmark.timings.push({
      step: createRelated.label,
      ms: createRelated.ms,
      status: createRelated.result.status,
    });
    console.log(
      "create related index:",
      createRelated.result.status,
      createRelated.result.data
    );

    // 2) Seed related documents with custom IDs
    const parents = [
      { sku: "sku-1", title: "Parent A", body: "root node" },
      { sku: "sku-2", title: "Parent B", body: "root node B" },
    ];
    if (faviconBase64) {
      for (const parent of parents) {
        parent.favicon = {
          format: "ico",
          encoding: "base64",
          content: faviconBase64,
        };
      }
    }
    for (const doc of parents) {
      const res = await indexDoc(relatedIndex, doc);
      if (res.status !== 201) throw new Error("Failed to index parent");
      benchmark.docsIndexed[relatedIndex] += 1;
      console.log("index parent:", res.data);
    }

    // 3) Index documents in primary index referencing parents
    const docs = [
      {
        title: "Doc1",
        body: "quick brown fox jumps over the lazy dog",
        tags: ["animal", "fast"],
        labels: ["short"],
        flag: true,
        priority: 5,
        vec: [1, 0, 0],
        parent: { id: "sku-1", index: relatedIndex },
      },
      {
        title: "Doc2",
        body: "fast red fox leaped high",
        tags: ["animal"],
        labels: ["medium"],
        flag: false,
        priority: 3,
        vec: [0.9, 0.1, 0],
        parent: { id: "sku-1", index: relatedIndex },
      },
      {
        title: "Doc3",
        body: "sleepy dog rests quietly",
        tags: ["animal", "sleep"],
        labels: ["short"],
        flag: true,
        priority: 1,
        vec: [0, 1, 0],
        parent: { id: "sku-2", index: relatedIndex },
      },
    ];
    const docTimings = [];
    for (const doc of docs) {
      const step = await timeStep(`index_${doc.title}`, async () =>
        indexDoc(primaryIndex, doc)
      );
      docTimings.push({
        doc: doc.title,
        ms: step.ms,
        status: step.result.status,
      });
      if (step.result.status === 201) {
        benchmark.docsIndexed[primaryIndex] += 1;
      }
      console.log("index doc:", step.result.status, step.result.data);
    }
    benchmark.timings.push({
      step: "index_documents_total",
      ms: docTimings.reduce((s, t) => s + t.ms, 0),
      status: "aggregate",
      perDoc: docTimings,
    });

    // 4) Partial update using string ID on related index
    const patchStep = await timeStep("patch_parent", async () =>
      patchDoc(relatedIndex, "sku-2", { body: "updated root" })
    );
    benchmark.timings.push({
      step: patchStep.label,
      ms: patchStep.ms,
      status: patchStep.result.status,
    });
    console.log("patch parent:", patchStep.result.status, patchStep.result.data);

    // 5) Bulk ingest extra docs in secondary index to test multi-index load
    const bulkCount = parseInt(process.env.BULK_COUNT || "200", 10);
    const vocab = [
      "fast",
      "slow",
      "quick",
      "brown",
      "fox",
      "dog",
      "cat",
      "red",
      "blue",
      "green",
      "run",
      "jump",
      "sleep",
      "quiet",
      "loud",
      "sky",
      "river",
      "mountain",
      "forest",
      "code",
      "data",
    ];
    const bulkDocs = Array.from({ length: bulkCount }).map((_, i) => {
      const body = randomText(vocab, 8 + Math.floor(Math.random() * 10));
      const title = `RelatedBulk${i + 1}`;
      return {
        sku: `bulk-${i + 1}`,
        title,
        body,
      };
    });
    const bulkStart = hrMs();
    for (const doc of bulkDocs) {
      const res = await indexDoc(relatedIndex, doc);
      if (res.status === 201) {
        benchmark.docsIndexed[relatedIndex] += 1;
      }
    }
    const bulkMs = hrMs() - bulkStart;
    benchmark.timings.push({
      step: "bulk_related",
      ms: bulkMs,
      status: "aggregate",
      docs: bulkCount,
    });
    console.log(
      `bulk index (related): ${bulkCount} docs in ${bulkMs.toFixed(2)} ms`
    );

    // 6) Searches (baseline + relation embeddings)
    const queries = [
      { name: "bm25", params: { q: "quick fox", mode: "bm25" } },
      {
        name: "bm25_paged",
        params: { q: "quick brown fox", mode: "bm25", from: 1, size: 2 },
      },
      {
        name: "bm25_filters",
        params: {
          q: "fox",
          mode: "bm25",
          tag: "animal",
          label: "short",
          flag: "true",
          filter_priority_min: 1,
          filter_priority_max: 6,
        },
      },
      {
        name: "lexical_array_filter",
        params: {
          q: "quiet",
          mode: "lexical",
          filter_labels: "short",
        },
      },
      {
        name: "fuzzy",
        params: { q: "quik fox", mode: "fuzzy", distance: 2 },
      },
      {
        name: "hybrid_weighted",
        params: {
          q: "quick fox",
          mode: "hybrid",
          w_bm25: 0.7,
          w_semantic: 1.5,
          w_lexical: 0.3,
        },
      },
      { name: "semantic", params: { q: "quick brown fox", mode: "semantic" } },
      {
        name: "bm25_relation_inline",
        params: { q: "fox", mode: "bm25", include_relations: "inline" },
      },
      {
        name: "bm25_relation_hierarchy",
        params: {
          q: "fox",
          mode: "bm25",
          include_relations: "hierarchy",
          max_relation_depth: 2,
        },
      },
      {
        name: "bm25_relation_cross_index",
        params: {
          q: "root",
          mode: "bm25",
          include_relations: "inline",
        },
      },
      {
        name: "bm25_related_index",
        index: relatedIndex,
        params: { q: "root", mode: "bm25", size: 5 },
      },
      {
        name: "related_custom_id_eq",
        index: relatedIndex,
        params: { q: "Parent", mode: "bm25", filter_sku: "sku-1" },
      },
      {
        name: "vector",
        params: { q: "vector", mode: "vector", vec: "1,0,0" },
      },
    ];

    for (const q of queries) {
      const idx = q.index || primaryIndex;
      const step = await timeStep(`search_${q.name}`, async () =>
        runSearch(idx, q.params)
      );
      const payload = step.result.data || {};
      const hits =
        payload.data && payload.data.hits ? payload.data.hits.length : 0;
      benchmark.queries.push({
        targetIndex: idx,
        name: q.name,
        params: q.params,
        ms: step.ms,
        status: step.result.status,
        hits,
        total: payload.data ? payload.data.total : undefined,
      });
      console.log(
        `search (${q.name}@${idx}):`,
        step.result.status,
        JSON.stringify(payload, null, 2)
      );
    }

    // 7) Fetch + delete using custom IDs
    const fetchParent = await timeStep("fetch_parent_sku", async () =>
      fetchDoc(relatedIndex, "sku-1")
    );
    benchmark.timings.push({
      step: fetchParent.label,
      ms: fetchParent.ms,
      status: fetchParent.result.status,
    });
    console.log("fetch parent:", fetchParent.result.status, fetchParent.result.data);

    const deleteParent = await timeStep("delete_parent_sku", async () =>
      deleteDoc(relatedIndex, "sku-2")
    );
    benchmark.timings.push({
      step: deleteParent.label,
      ms: deleteParent.ms,
      status: deleteParent.result.status,
    });
    console.log("delete parent:", deleteParent.result.status, deleteParent.result.data);

    // 9) Custom aggregation API (pages + related docs)
    const customApiName = "websearch";
    const customSpec = {
      base_index: primaryIndex,
      select: ["title", "body", "priority"],
      relations: [
        {
          name: "site",
          field: "parent",
          target_index: relatedIndex,
          select: ["title", "body", "favicon"],
          include_image: true,
          image_field: "favicon",
        },
      ],
    };
    const customCreate = await timeStep("custom_api_create", async () =>
      axios.put(`/v1/custom/${customApiName}`, customSpec, {
        headers: { "Content-Type": "application/json" },
      })
    );
    benchmark.timings.push({
      step: customCreate.label,
      ms: customCreate.ms,
      status: customCreate.result.status,
    });
    console.log("custom api create:", customCreate.result.status, customCreate.result.data);

    const customQuery = await timeStep("custom_api_query", async () =>
      axios.post(`/v1/custom/${customApiName}`, {
        q: "fox",
        mode: "bm25",
        size: 3,
      })
    );
    benchmark.timings.push({
      step: customQuery.label,
      ms: customQuery.ms,
      status: customQuery.result.status,
    });
    console.log(
      "custom api query:",
      customQuery.result.status,
      JSON.stringify(customQuery.result.data, null, 2)
    );

    // 8) Persist benchmark JSON
    const outPath = path.join(__dirname, "benchmark_results.json");
    fs.writeFileSync(outPath, JSON.stringify(benchmark, null, 2), "utf8");
    console.log(`Benchmark results written to ${outPath}`);
  } catch (err) {
    console.error("Test failed:", err.message);
  }
}

main();
