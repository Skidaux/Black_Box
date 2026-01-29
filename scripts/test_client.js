// Interactive CLI test runner for BlackBox HTTP API.
// Run: node scripts/test_client.js
// Presents a menu for: benchmark, durability, stress.
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const readline = require("readline");

const axios = require("axios").create({
  baseURL: "http://127.0.0.1:8080",
  timeout: 15000,
  validateStatus: () => true,
});

const hrMs = () => Number(process.hrtime.bigint()) / 1e6;
const randChoice = (arr) => arr[Math.floor(Math.random() * arr.length)];
const randomText = (words, len) => {
  let out = [];
  for (let i = 0; i < len; ++i) out.push(randChoice(words));
  return out.join(" ");
};
let pathing = [];
const platfromOs = process.platform;
if (platfromOs == "win32") {
  pathing = ["Release", "BlackBox.exe"];
} else if (platfromOs == "linux") {
  pathing = ["", "BlackBox"];
}

const serverCmd =
  // process.env.BLACKBOX_SERVER ||
  path.join("build", pathing[0], pathing[1]);
const serverCwd = path.resolve(__dirname, "..");
let serverSeq = 0;
const restartDelayMs = parseInt(process.env.BLACKBOX_RESTART_DELAY_MS || "500", 10);
const nowIso = () => new Date().toISOString();

async function timeStep(label, fn) {
  const start = hrMs();
  const result = await fn();
  const end = hrMs();
  return { label, ms: end - start, result };
}

async function ensureIndex(name, schema) {
  const res = await axios.post("/v1/indexes", { name, schema });
  if (res.status !== 201 && res.status !== 400) {
    throw new Error(`Failed to create index ${name}: status=${res.status}`);
  }
  return res;
}

async function indexDoc(index, doc) {
  return axios.post(`/v1/${index}/doc`, doc, {
    headers: { "Content-Type": "application/json" },
  });
}

async function bulkIndex(index, docs, continueOnError = true) {
  return axios.post(
    `/v1/${index}/_bulk?continue_on_error=${continueOnError}`,
    docs,
    { headers: { "Content-Type": "application/json" } }
  );
}

async function patchDoc(index, id, body) {
  return axios.patch(`/v1/${index}/doc/${id}`, body, {
    headers: { "Content-Type": "application/json" },
  });
}

async function runSearch(index, params) {
  const qs = new URLSearchParams(params).toString();
  return axios.get(`/v1/${index}/search?${qs}`);
}

async function waitForServer(timeoutMs = 20000) {
  const start = Date.now();
  let lastError = null;
  const debug = process.env.DEBUG_WAIT === "1";
  let attempts = 0;
  while (Date.now() - start < timeoutMs) {
    attempts += 1;
    try {
      // Use a short per-request timeout so a stalled connection doesn't consume the full wait window.
      const res = await axios.get("/v1/health", { timeout: 2000 });
      if (res.status === 200) {
        if (debug) console.log(`[wait] healthy after ${attempts} attempts`);
        return;
      }
      lastError = `status=${res.status}`;
    } catch (e) {
      lastError = e.code || e.message;
    }
    if (debug) {
      console.log(`[wait] attempt ${attempts} failed: ${lastError}`);
      if (attempts === 5) {
        try {
          const lsof = require("child_process").execSync("lsof -i :8080 || true", {
            encoding: "utf8",
          });
          console.log(`[wait] lsof :8080\n${lsof}`);
        } catch (e) {
          console.log(`[wait] lsof failed: ${e.message}`);
        }
      }
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  const suffix = lastError ? ` (last error: ${lastError})` : "";
  throw new Error(`Server did not become healthy in time${suffix}`);
}

function startServerProcess() {
  const proc = spawn(serverCmd, [], {
    cwd: serverCwd,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const seq = ++serverSeq;
  console.log(`[server ${seq}] spawned pid=${proc.pid}`);
  proc.stdout.on("data", (chunk) =>
    process.stdout.write(`[BlackBox ${seq}] ${chunk.toString()}`)
  );
  proc.stderr.on("data", (chunk) =>
    process.stderr.write(`[BlackBox ${seq}] ${chunk.toString()}`)
  );
  proc.once("exit", (code, signal) =>
    console.log(
      `[server ${seq}] exited at ${nowIso()} code=${code} signal=${signal}`
    )
  );
  return proc;
}

async function startServerAndWait(label = "server", retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const proc = startServerProcess();
    try {
      await waitForServer();
      if (process.env.DEBUG_WAIT === "1") {
        console.log(`[${label}] healthy on attempt ${attempt}`);
      }
      return proc;
    } catch (err) {
      if (process.env.DEBUG_WAIT === "1") {
        console.log(
          `[${label}] start attempt ${attempt} failed: ${err.message}`
        );
      }
      await stopServerProcess(proc);
      if (attempt === retries) throw err;
      if (restartDelayMs > 0)
        await new Promise((r) => setTimeout(r, restartDelayMs));
    }
  }
}

function stopServerProcess(proc) {
  if (!proc) return Promise.resolve();

  // If the child already exited (exitCode is set), resolve immediately so callers don't hang.
  if (proc.exitCode !== null || proc.signalCode !== null) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    let settled = false;
    let timeoutId = null;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (timeoutId !== null) clearTimeout(timeoutId);
      resolve();
    };

    proc.once("exit", finish);
    proc.once("close", finish);

    // Try graceful shutdown first, then hard kill after a short delay.
    proc.kill("SIGTERM");
    timeoutId = setTimeout(() => {
      if (!proc.killed) proc.kill("SIGKILL");
      finish();
    }, 2000);
  });
}

async function runBenchmark() {
  const primaryIndex = "demo";
  const relatedIndex = "related_demo";
  const benchmark = {
    timestamp: new Date().toISOString(),
    baseURL: axios.defaults.baseURL,
    indexes: [primaryIndex, relatedIndex],
    docsIndexed: { [primaryIndex]: 0, [relatedIndex]: 0 },
    timings: [],
    queries: [],
    notes:
      "Benchmark: multi-index workflow, bulk ingest, vector/lexical/fuzzy/hybrid searches, relation testing.",
  };

  try {
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
    };
    const relatedSchema = {
      fields: { title: "text", body: "text", sku: "text" },
      doc_id: { field: "sku", type: "string" },
    };

    const createPrimary = await timeStep("create_primary", () =>
      ensureIndex(primaryIndex, primarySchema)
    );
    benchmark.timings.push({
      step: createPrimary.label,
      ms: createPrimary.ms,
      status: createPrimary.result.status,
    });
    console.log("create primary index:", createPrimary.result.status);

    const createRelated = await timeStep("create_related", () =>
      ensureIndex(relatedIndex, relatedSchema)
    );
    benchmark.timings.push({
      step: createRelated.label,
      ms: createRelated.ms,
      status: createRelated.result.status,
    });
    console.log("create related index:", createRelated.result.status);

    const parents = [
      { sku: "sku-1", title: "Parent A", body: "root node" },
      { sku: "sku-2", title: "Parent B", body: "root node B" },
    ];
    for (const doc of parents) {
      const res = await indexDoc(relatedIndex, doc);
      if (res.status === 201) benchmark.docsIndexed[relatedIndex] += 1;
      console.log("index parent:", doc.sku, res.status);
    }

    const docs = [
      {
        title: "Doc1",
        body: "quick brown fox jumps over the lazy dog",
        parent: { id: "sku-1", index: relatedIndex },
        tags: ["animal", "fast"],
        labels: ["short"],
        flag: true,
        priority: 5,
        vec: [1, 0, 0],
      },
      {
        title: "Doc2",
        body: "fast red fox leaped high",
        parent: { id: "sku-1", index: relatedIndex },
        tags: ["animal"],
        labels: ["medium"],
        flag: false,
        priority: 3,
        vec: [0.9, 0.1, 0],
      },
      {
        title: "Doc3",
        body: "sleepy dog rests quietly",
        parent: { id: "sku-2", index: relatedIndex },
        tags: ["animal", "sleep"],
        labels: ["short"],
        flag: true,
        priority: 1,
        vec: [0, 1, 0],
      },
    ];

    const docTimings = [];
    for (const doc of docs) {
      const step = await timeStep(`index_${doc.title}`, () =>
        indexDoc(primaryIndex, doc)
      );
      docTimings.push({
        doc: doc.title,
        ms: step.ms,
        status: step.result.status,
      });
      if (step.result.status === 201) benchmark.docsIndexed[primaryIndex] += 1;
    }
    benchmark.timings.push({
      step: "index_documents_total",
      ms: docTimings.reduce((s, t) => s + t.ms, 0),
      status: "aggregate",
      perDoc: docTimings,
    });

    const patchStep = await timeStep("patch_parent", () =>
      patchDoc(relatedIndex, "sku-2", { body: "updated root" })
    );
    benchmark.timings.push({
      step: patchStep.label,
      ms: patchStep.ms,
      status: patchStep.result.status,
    });
    console.log("patch parent:", patchStep.result.status);

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
    const bulkDocs = Array.from({ length: bulkCount }).map((_, i) => ({
      sku: `bulk-${i + 1}`,
      title: `RelatedBulk${i + 1}`,
      body: randomText(vocab, 9 + Math.floor(Math.random() * 8)),
    }));
    const bulkStart = hrMs();
    for (const doc of bulkDocs) {
      const res = await indexDoc(relatedIndex, doc);
      if (res.status === 201) benchmark.docsIndexed[relatedIndex] += 1;
    }
    benchmark.timings.push({
      step: "bulk_related",
      ms: hrMs() - bulkStart,
      status: "aggregate",
      docs: bulkCount,
    });

    const queries = [
      { name: "bm25", params: { q: "quick fox", mode: "bm25" } },
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
        name: "hybrid",
        params: {
          q: "quick fox",
          mode: "hybrid",
          w_bm25: 0.7,
          w_semantic: 1.2,
          w_lexical: 0.4,
        },
      },
      {
        name: "vector",
        params: { q: "vector", mode: "vector", vec: "1,0,0", size: 3 },
      },
      {
        name: "bulk_search",
        index: relatedIndex,
        params: { q: "bulk", mode: "bm25", size: 5 },
      },
      {
        name: "relation_parent",
        index: relatedIndex,
        params: { q: "Parent", mode: "bm25", size: 2 },
      },
    ];
    for (const q of queries) {
      const idx = q.index || primaryIndex;
      const step = await timeStep(`search_${q.name}`, () =>
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
    }

    // Relation fetch: get child doc and resolve its parent from relatedIndex
    try {
      const childRes = await axios.get(`/v1/${primaryIndex}/doc/1`);
      if (childRes.status === 200) {
        const parent = childRes.data?.data?.doc?.parent;
        if (parent && parent.id) {
          const parentDoc = await axios.get(
            `/v1/${relatedIndex}/doc/${parent.id}`
          );
          benchmark.queries.push({
            name: "relation_parent_fetch",
            params: { child: 1, parent: parent.id },
            status: parentDoc.status,
            parentTitle: parentDoc.data?.data?.doc?.title,
          });
        }
      }
    } catch (e) {
      benchmark.queries.push({
        name: "relation_parent_fetch",
        status: "error",
        error: e.message,
      });
    }

    const outPath = path.join(__dirname, "benchmark_results.json");
    fs.writeFileSync(outPath, JSON.stringify(benchmark, null, 2), "utf8");
    console.log(`Benchmark results written to ${outPath}`);
  } catch (err) {
    console.error("Benchmark failed:", err.message);
  }
}

async function runQueryValueTest() {
  const index = `query_meta_${Date.now()}`;
  const schema = {
    fields: {
      title: "text",
      version: { type: "number", searchable: false },
      queries: { type: "query_values", searchable: true },
    },
  };
  const results = { index, status: "pending", steps: [] };
  try {
    let res = await ensureIndex(index, schema);
    results.steps.push({ step: "create_index", status: res.status });

    res = await indexDoc(index, {
      title: "YouTube root",
      version: 1,
      queries: [{ query: "yt", score: 1.0 }, { query: "video", score: 0.6 }],
    });
    const ytId = res.data?.data?.id;
    results.steps.push({ step: "index_doc", status: res.status, id: ytId });

    res = await runSearch(index, { q: "version", mode: "bm25" });
    const versionHits = res.data?.data?.hits ?? [];
    results.steps.push({ step: "search_version", status: res.status, hits: versionHits.length });

    res = await runSearch(index, { q: "yt", mode: "bm25" });
    const ytHits = res.data?.data?.hits ?? [];
    const topId = ytHits[0]?.id;
    results.steps.push({ step: "search_yt", status: res.status, topId });

    const sm = await axios.get(`/v1/${index}/stored_match?field=version&value=1`);
    results.steps.push({ step: "stored_match_version", status: sm.status, hits: sm.data?.data?.hits?.length ?? 0 });

    results.status = "ok";
    console.log("Query-value test results:", results);
  } catch (e) {
    results.status = "error";
    results.error = e.message;
    console.error("Query-value test failed:", e);
  } finally {
    const outPath = path.join(__dirname, "query_value_results.json");
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Query-value results written to ${outPath}`);
  }
}

async function runFullFeatureTest() {
  const sitesIndex = `sites_${Date.now()}`;
  const pagesIndex = `pages_${Date.now()}`;
  const favicon = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mO0dOjYfwAJ7APc6+UeFwAAAABJRU5ErkJggg==";
  const schemaSites = {
    fields: {
      title: "text",
      body: "text",
      domain: "text",
      tags: "array",
      labels: "array",
      flag: "bool",
      priority: "number",
      vec: { type: "vector", dim: 3 },
      favicon: { type: "image", max_kb: 4 },
    },
    doc_id: { field: "domain", type: "string" },
  };
  const schemaPages = {
    fields: {
      title: "text",
      body: "text",
      tags: "array",
      labels: "array",
      flag: "bool",
      priority: "number",
      vec: { type: "vector", dim: 3 },
      version: { type: "number", searchable: false },
      queries: { type: "query_values", searchable: true },
    },
    relation: { field: "site_ref", target_index: sitesIndex },
  };

  const results = { status: "pending", steps: [] };
  try {
    let res = await ensureIndex(sitesIndex, schemaSites);
    results.steps.push({ step: "create_sites", status: res.status });
    res = await ensureIndex(pagesIndex, schemaPages);
    results.steps.push({ step: "create_pages", status: res.status });

    const siteDoc = {
      domain: "youtube.com",
      title: "YouTube",
      body: "Video platform",
      tags: ["video", "platform"],
      labels: ["root"],
      flag: true,
      priority: 10,
      vec: [0.9, 0.05, 0.05],
      favicon: { format: "png", encoding: "base64", content: favicon },
    };
    res = await indexDoc(sitesIndex, siteDoc);
    const siteId = res.data?.data?.id;
    results.steps.push({ step: "index_site", status: res.status, id: siteId });

    const pageDoc = {
      title: "YouTube Home",
      body: "Welcome to YouTube",
      tags: ["video", "home"],
      labels: ["landing"],
      flag: true,
      priority: 9,
      vec: [0.85, 0.1, 0.05],
      version: 1,
      queries: [{ query: "yt", score: 1.0 }, { query: "youtube", score: 0.9 }],
      site_ref: { id: "youtube.com", index: sitesIndex },
    };
    res = await indexDoc(pagesIndex, pageDoc);
    const pageId = res.data?.data?.id;
    results.steps.push({ step: "index_page", status: res.status, id: pageId });

    const altPageDoc = {
      title: "YouTube About",
      body: "Learn about YouTube",
      tags: ["video", "about"],
      labels: ["info"],
      flag: false,
      priority: 5,
      vec: [0.8, 0.15, 0.05],
      version: 2,
      queries: [{ query: "youtube", score: 0.7 }],
      site_ref: { id: "youtube.com", index: sitesIndex },
    };
    const altRes = await indexDoc(pagesIndex, altPageDoc);
    const altPageId = altRes.data?.data?.id;
    results.steps.push({ step: "index_page_alt", status: altRes.status, id: altPageId });

    // Search should return the page boosted by query_values and include relation + favicon metadata
    const search = await runSearch(pagesIndex, {
      q: "yt",
      mode: "bm25",
      include_relations: "inline",
    });
    const hits = search.data?.data?.hits ?? [];
    const top = hits[0] || {};
    const relFaviconBytes =
      top.relation && top.relation.doc && top.relation.doc.favicon
        ? top.relation.doc.favicon.bytes
        : undefined;
    results.steps.push({
      step: "search_pages",
      status: search.status,
      topId: top.id,
      hitsCount: hits.length,
      hitsFull: hits,
      relationIndex: top.relation ? top.relation.index : null,
      faviconBytes: relFaviconBytes,
    });

    // Stored-match should find the version field even though it's non-searchable
    const sm = await axios.get(`/v1/${pagesIndex}/stored_match?field=version&value=1`);
    results.steps.push({ step: "stored_match_version", status: sm.status, hits: sm.data?.data?.hits?.length ?? 0 });

    results.status = "ok";
    const outPath = path.join(__dirname, "full_feature_results.json");
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Full feature test results written to ${outPath}`);
  } catch (e) {
    results.status = "error";
    results.error = e.message;
    console.error("Full feature test failed:", e);
  }
}

async function runDurability() {
  const testIndex = `durability_${Date.now()}`;
  const relIndex = `durability_rel_${Date.now()}`;
  const favicon = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mO0dOjYfwAJ7APc6+UeFwAAAABJRU5ErkJggg==";
  const results = {
    timestamp: new Date().toISOString(),
    index: testIndex,
    relIndex,
    server: serverCmd,
    steps: [],
  };

  let proc = null;
  try {
    proc = await startServerAndWait("durability:start");
    results.steps.push({ step: "server_start", status: "ok" });
    await ensureIndex(testIndex, {
      fields: {
        title: "text",
        body: "text",
        vec: { type: "vector", dim: 3 },
        version: { type: "number", searchable: false },
      },
    });
    await ensureIndex(relIndex, {
      fields: {
        title: "text",
        favicon: { type: "image", max_kb: 4 },
        domain: "text",
      },
      doc_id: { field: "domain", type: "string" },
    });
    const doc = {
      title: "durable",
      body: "persist across restarts",
      vec: [0.5, 0.1, 0.3],
      version: 1,
    };
    const inserted = await indexDoc(testIndex, doc);
    const docId =
      inserted.data && inserted.data.data ? inserted.data.data.id : undefined;
    if (!docId || inserted.status !== 201) {
      throw new Error(
        `Durability doc missing id (status=${inserted.status}, body=${JSON.stringify(
          inserted.data
        )})`
      );
    }
    results.steps.push({
      step: "index_doc",
      id: docId,
      status: inserted.status,
    });

    // Update document to test persistence of updates
    const patchRes = await patchDoc(testIndex, docId, { body: "persist updated", version: 2 });
    results.steps.push({ step: "patch_doc", status: patchRes.status });

    // Index related doc with image + custom id
    const relRes = await indexDoc(relIndex, {
      title: "Site Root",
      domain: "example.com",
      favicon: { format: "png", encoding: "base64", content: favicon },
    });
    const relId = relRes.data?.data?.id;
    results.steps.push({ step: "index_rel_doc", status: relRes.status, id: relId });

    // Force snapshot so data is persisted beyond WAL for tiny workloads
    try {
      const snap = await axios.post("/v1/snapshot");
      results.steps.push({ step: "snapshot", status: snap.status });
    } catch (e) {
      results.steps.push({
        step: "snapshot",
        status: "failed",
        error: e.message,
      });
    }

    await stopServerProcess(proc);
    results.steps.push({ step: "shutdown", status: "ok" });

    // Give the OS a moment to release the listening socket before restarting.
    if (restartDelayMs > 0) await new Promise((r) => setTimeout(r, restartDelayMs));
    proc = await startServerAndWait("durability:restart");
    results.steps.push({ step: "restart", status: "ok" });

    const fetched = await axios.get(`/v1/${testIndex}/doc/${docId}`);
    results.steps.push({
      step: "fetch_after_restart",
      status: fetched.status === 200 ? "ok" : "failure",
      payload: fetched.data,
    });
    const search = await runSearch(testIndex, { q: "persist", mode: "bm25" });
    results.steps.push({
      step: "search_after_restart",
      status: search.status,
      hits: search.data?.data?.hits?.length ?? 0,
    });
    const relFetch = await axios.get(`/v1/${relIndex}/doc/example.com`);
    results.steps.push({
      step: "fetch_rel_after_restart",
      status: relFetch.status,
      payload: relFetch.data,
    });

    // Delete and ensure tombstone survives restart
    const del = await axios.delete(`/v1/${testIndex}/doc/${docId}`);
    results.steps.push({ step: "delete_doc", status: del.status });
    // Persist snapshot with tombstone
    try {
      const snapDel = await axios.post("/v1/snapshot");
      results.steps.push({ step: "snapshot_after_delete", status: snapDel.status });
    } catch (e) {
      results.steps.push({ step: "snapshot_after_delete", status: "failed", error: e.message });
    }
    await stopServerProcess(proc);
    results.steps.push({ step: "shutdown_post_delete", status: "ok" });
    if (restartDelayMs > 0) await new Promise((r) => setTimeout(r, restartDelayMs));
    proc = await startServerAndWait("durability:restart_after_delete");
    results.steps.push({ step: "restart_after_delete", status: "ok" });
    const fetchAfterDelete = await axios.get(`/v1/${testIndex}/doc/${docId}`);
    results.steps.push({
      step: "fetch_after_delete_restart",
      status: fetchAfterDelete.status,
      payload: fetchAfterDelete.data,
    });
    const searchAfterDelete = await runSearch(testIndex, {
      q: "persist",
      mode: "bm25",
    });
    results.steps.push({
      step: "search_after_delete_restart",
      status: searchAfterDelete.status,
      hits: searchAfterDelete.data?.data?.hits?.length ?? 0,
    });

    // Corrupt WAL tail simulation: append garbage and ensure we still start
    const walPath = path.join(serverCwd, "data", `${testIndex}.wal`);
    try {
      fs.appendFileSync(walPath, "CORRUPTTAIL");
      results.steps.push({ step: "wal_corrupt_tail", status: "ok" });
    } catch (e) {
      results.steps.push({
        step: "wal_corrupt_tail",
        status: "failed",
        error: e.message,
      });
    }
    await stopServerProcess(proc);
    results.steps.push({ step: "shutdown_after_corrupt", status: "ok" });
    if (restartDelayMs > 0) await new Promise((r) => setTimeout(r, restartDelayMs));
    proc = await startServerAndWait("durability:restart_after_corrupt");
    results.steps.push({ step: "restart_after_corrupt", status: "ok" });
    const health = await axios.get("/v1/health");
    results.steps.push({ step: "health_after_corrupt", status: health.status });
  } catch (err) {
    results.steps.push({ step: "error", message: err.message });
    console.error("Durability test failed:", err.message);
  } finally {
    await stopServerProcess(proc);
    const outPath = path.join(__dirname, "durability_results.json");
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Durability results written to ${outPath}`);
  }
}

async function runStress(maxDocs, concurrency, batchSize = 200) {
  const index = `stress_${Date.now()}`;
  const results = {
    timestamp: new Date().toISOString(),
    index,
    targetDocs: maxDocs,
    concurrency,
    batchSize,
    successes: 0,
    failures: 0,
    firstError: null,
    durationMs: 0,
  };
  try {
    await ensureIndex(index, { fields: { title: "text", body: "text" } });
    const vocab = [
      "alpha",
      "beta",
      "gamma",
      "delta",
      "epsilon",
      "zeta",
      "eta",
      "theta",
      "lambda",
    ];
    let counter = 0;
    let failed = false;
    const start = hrMs();
    async function worker() {
      while (!failed) {
        let startId;
        let docs = [];
        // reserve next batch atomically-ish
        if (counter >= maxDocs) break;
        startId = counter + 1;
        const remaining = maxDocs - counter;
        const take = Math.min(batchSize, remaining);
        counter += take;
        for (let i = 0; i < take; ++i) {
          const id = startId + i;
          docs.push({ title: `Doc ${id}`, body: randomText(vocab, 12) });
        }
        try {
          const res = await bulkIndex(index, docs, true);
          const okIds = res.data?.data?.ids || res.data?.ids || [];
          const errs = res.data?.data?.errors || res.data?.errors || [];
          results.successes += okIds.length;
          results.failures += errs.length + Math.max(0, take - okIds.length - errs.length);
          if (res.status !== 201 && res.status !== 207 && !results.firstError) {
            results.firstError = { status: res.status, data: res.data };
            failed = true;
            break;
          }
          if (errs.length && !results.firstError) {
            results.firstError = { status: res.status, errors: errs };
          }
        } catch (err) {
          results.failures += take;
          if (!results.firstError) results.firstError = { error: err.message };
          failed = true;
          break;
        }
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));
    results.durationMs = hrMs() - start;
    const search = await runSearch(index, {
      q: "alpha",
      mode: "bm25",
      size: 1,
    });
    results.searchStatus = search.status;
    results.searchHits = search.data?.data?.hits?.length ?? 0;
  } catch (err) {
    results.failures += 1;
    if (!results.firstError) results.firstError = { error: err.message };
  } finally {
    const outPath = path.join(__dirname, "stress_results.json");
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Stress results written to ${outPath}`);
  }
}

async function runSnapshotTest() {
  const name = `snapshot_${Date.now()}`;
  const snapshotDir = path.join(__dirname, "snapshots");
  const snapshotPath = path.join(snapshotDir, `${name}.manifest`);
  const results = {
    timestamp: new Date().toISOString(),
    index: name,
    snapshotPath,
    steps: [],
  };
  let proc = null;
  try {
    proc = await startServerAndWait("snapshot:start");
    results.steps.push({ step: "server_start", status: "ok" });
    await ensureIndex(name, {
      fields: {
        title: "text",
        body: "text",
        version: { type: "number", searchable: false },
      },
    });
    const docRes = await indexDoc(name, {
      title: "snap",
      body: "persist me",
      version: 1,
    });
    const docId = docRes.data?.data?.id;
    results.steps.push({ step: "index_doc", status: docRes.status, id: docId });

    // Save snapshot to a custom path
    fs.mkdirSync(snapshotDir, { recursive: true });
    const snapRes = await axios.post(`/v1/snapshot?path=${encodeURIComponent(snapshotPath)}`);
    results.steps.push({ step: "snapshot_save", status: snapRes.status });

    // Mutate state (delete doc) to ensure load restores it
    await axios.delete(`/v1/${name}/doc/${docId}`);
    results.steps.push({ step: "delete_doc", status: "ok" });

    // Stop server and wipe data dir to force load from snapshot
    await stopServerProcess(proc);
    proc = null;
    results.steps.push({ step: "shutdown", status: "ok" });
    try {
      fs.rmSync(path.join(serverCwd, "data"), { recursive: true, force: true });
      results.steps.push({ step: "wipe_data", status: "ok" });
    } catch (e) {
      results.steps.push({ step: "wipe_data", status: "failed", error: e.message });
    }

    // Restart and load from snapshot
    if (restartDelayMs > 0) await new Promise((r) => setTimeout(r, restartDelayMs));
    proc = await startServerAndWait("snapshot:restart");
    results.steps.push({ step: "restart", status: "ok" });
    const loadRes = await axios.post(`/v1/snapshot/load?path=${encodeURIComponent(snapshotPath)}`);
    results.steps.push({ step: "snapshot_load", status: loadRes.status });

    const search = await runSearch(name, { q: "persist", mode: "bm25" });
    results.steps.push({
      step: "search_after_load",
      status: search.status,
      hits: search.data?.data?.hits?.length ?? 0,
    });
    const stored = await axios.get(`/v1/${name}/stored_match?field=version&value=1`);
    results.steps.push({
      step: "stored_match_after_load",
      status: stored.status,
      hits: stored.data?.data?.hits?.length ?? 0,
    });
    results.status = "ok";
  } catch (e) {
    results.status = "error";
    results.error = e.message;
    console.error("Snapshot test failed:", e);
  } finally {
    await stopServerProcess(proc);
    const outPath = path.join(__dirname, "snapshot_results.json");
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.mkdirSync(snapshotDir, { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Snapshot test results written to ${outPath}`);
  }
}

async function runCoverageTest() {
  process.env.BLACKBOX_SHARD_ID = "shard-js";
  process.env.BLACKBOX_REPLICA_ID = "replica-js";
  process.env.BLACKBOX_SHIP_ENDPOINT = "http://ship-js";
  process.env.BLACKBOX_SHIP_METHOD = "http";
  const index = `coverage_${Date.now()}`;
  const snapshotDir = path.join(__dirname, "snapshots");
  const snapshotPath = path.join(snapshotDir, `${index}.manifest`);
  const results = { timestamp: new Date().toISOString(), index, steps: [] };
  let proc = null;
  try {
    proc = await startServerAndWait("coverage:start");
    results.steps.push({ step: "server_start", status: "ok" });

    const createRes = await ensureIndex(index, {
      fields: {
        title: "text",
        body: "text",
        priority: { type: "number", searchable: false },
        vec: { type: "vector", dim: 2 },
      },
    });
    results.steps.push({ step: "create_index", status: createRes.status });

    const docA = await indexDoc(index, {
      title: "DocA",
      body: "quick red fox",
      priority: 5,
      vec: [1, 0],
    });
    const docAId = docA.data?.data?.id;
    results.steps.push({ step: "index_doc_a", status: docA.status, id: docAId });

    const docB = await indexDoc(index, {
      title: "DocB",
      body: "slow dog",
      priority: 1,
      vec: [0, 1],
    });
    const docBId = docB.data?.data?.id;
    results.steps.push({ step: "index_doc_b", status: docB.status, id: docBId });

    const phrase = await runSearch(index, { q: "quick red", mode: "phrase", size: 2 });
    const phraseHits = phrase.data?.data?.hits ?? [];
    results.steps.push({ step: "phrase_search", status: phrase.status, hits: phraseHits.length, topId: phraseHits[0]?.id });

    const range = await runSearch(index, {
      q: "fox",
      mode: "bm25",
      range_field: "priority",
      range_min: 4,
      range_max: 6,
      size: 2,
    });
    const rangeHits = range.data?.data?.hits ?? [];
    results.steps.push({ step: "range_filter", status: range.status, hits: rangeHits.length, topId: rangeHits[0]?.id });

    const vec = await axios.get(
      `/v1/${index}/search?mode=vector&vec=1,0&ann_probes=5&ef_search=32&size=1`
    );
    const vecHits = vec.data?.data?.hits ?? [];
    results.steps.push({ step: "vector_search_override", status: vec.status, topId: vecHits[0]?.id });

    // Save snapshot and apply via ship/apply
    fs.mkdirSync(snapshotDir, { recursive: true });
    const snapSave = await axios.post(`/v1/snapshot?path=${encodeURIComponent(snapshotPath)}`);
    results.steps.push({ step: "snapshot_save", status: snapSave.status });
    const shipApply = await axios.post(`/v1/ship/apply?path=${encodeURIComponent(snapshotPath)}`);
    results.steps.push({ step: "ship_apply", status: shipApply.status });

    // Fetch metrics to ensure new counters are exposed
    const metrics = await axios.get("/metrics");
    const hasDocCache = metrics.data.includes("blackbox_doc_cache_hits");
    const hasAnnRecall = metrics.data.includes("blackbox_ann_recall_samples");
    results.steps.push({ step: "metrics", status: metrics.status, docCacheMetric: hasDocCache, annRecallMetric: hasAnnRecall });

    const cfg = await axios.get("/v1/config");
    results.steps.push({
      step: "config",
      status: cfg.status,
      shard: cfg.data?.data?.shard_id,
      merge_throttle_ms: cfg.data?.data?.merge_throttle_ms,
    });

    const ship = await axios.get("/v1/ship");
    results.steps.push({
      step: "ship_plan",
      status: ship.status,
      shard: ship.data?.data?.cluster?.shard_id,
      replica: ship.data?.data?.cluster?.replica_id,
      indexes: (ship.data?.data?.indexes || []).length,
    });

    results.status = "ok";
  } catch (e) {
    results.status = "error";
    results.error = e.message;
    console.error("Coverage test failed:", e);
  } finally {
    await stopServerProcess(proc);
    const outPath = path.join(__dirname, "coverage_results.json");
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");
    console.log(`Coverage results written to ${outPath}`);
  }
}

function promptMenu() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve) => {
    console.log("Select a test to run:");
    console.log("  1) Benchmark (multi-index, bulk, searches)");
    console.log("  2) Durability (restart and verify persistence)");
    console.log("  3) Stress (bulk spam until limit)");
    console.log("  4) Query-values / non-searchable fields");
    console.log("  5) Full relations + images + query_values");
    console.log("  6) Snapshot save/load");
    console.log("  7) Config/ship plan + phrase/range/vector coverage");
    rl.question("Enter choice [1-7]: ", (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

function promptNumber(question, defaultVal) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve) => {
    rl.question(`${question} (default ${defaultVal}): `, (answer) => {
      rl.close();
      const v = parseInt(answer, 10);
      if (Number.isFinite(v) && v > 0) return resolve(v);
      resolve(defaultVal);
    });
  });
}

async function main() {
  const choice = await promptMenu();
  if (choice === "1") {
    await runBenchmark();
  } else if (choice === "2") {
    await runDurability();
  } else if (choice === "3") {
    const maxDocs = await promptNumber("Max documents to attempt", 5000);
    const concurrency = await promptNumber("Concurrency", 32);
    const batchSize = await promptNumber("Bulk batch size", 200);
    await runStress(maxDocs, concurrency, batchSize);
  } else if (choice === "4") {
    await runQueryValueTest();
  } else if (choice === "5") {
    await runFullFeatureTest();
  } else if (choice === "6") {
    await runSnapshotTest();
  } else if (choice === "7") {
    await runCoverageTest();
  } else {
    console.log("Invalid choice.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("CLI error:", err);
  process.exit(1);
});
