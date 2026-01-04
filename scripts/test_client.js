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

async function waitForServer(timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await axios.get("/v1/health");
      if (res.status === 200) return;
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error("Server did not become healthy in time");
}

function startServerProcess() {
  const proc = spawn(serverCmd, [], {
    cwd: serverCwd,
    stdio: ["ignore", "pipe", "pipe"],
  });
  proc.stdout.on("data", (chunk) =>
    process.stdout.write(`[BlackBox] ${chunk.toString()}`)
  );
  proc.stderr.on("data", (chunk) =>
    process.stderr.write(`[BlackBox] ${chunk.toString()}`)
  );
  return proc;
}

function stopServerProcess(proc) {
  if (!proc) return Promise.resolve();
  return new Promise((resolve) => {
    proc.once("exit", resolve);
    proc.kill();
    setTimeout(() => {
      if (!proc.killed) proc.kill("SIGKILL");
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

async function runDurability() {
  const testIndex = `durability_${Date.now()}`;
  const results = {
    timestamp: new Date().toISOString(),
    index: testIndex,
    server: serverCmd,
    steps: [],
  };

  let proc = null;
  try {
    proc = startServerProcess();
    await waitForServer();
    results.steps.push({ step: "server_start", status: "ok" });
    await ensureIndex(testIndex, {
      fields: { title: "text", body: "text", vec: { type: "vector", dim: 3 } },
    });
    const doc = {
      title: "durable",
      body: "persist across restarts",
      vec: [0.5, 0.1, 0.3],
    };
    const inserted = await indexDoc(testIndex, doc);
    const docId =
      inserted.data && inserted.data.data ? inserted.data.data.id : undefined;
    if (!docId) throw new Error("Durability doc missing id");
    results.steps.push({
      step: "index_doc",
      id: docId,
      status: inserted.status,
    });

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

    proc = startServerProcess();
    await waitForServer();
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
    proc = startServerProcess();
    await waitForServer();
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
    proc = startServerProcess();
    await waitForServer();
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
    rl.question("Enter choice [1-3]: ", (answer) => {
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
  } else {
    console.log("Invalid choice.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("CLI error:", err);
  process.exit(1);
});
