#!/usr/bin/env node
// Headless, assertion-based durability suite for BlackBox.
//
// Unlike test_client.js (interactive, dumps JSON for a human to eyeball), this
// runner asserts real invariants and exits non-zero when a HARD case fails, so
// it can gate CI. It exercises crash/restart, WAL-only replay, tail corruption,
// torn writes, tombstone survival and concurrent-write durability, and verifies
// document *content* after restart -- not just HTTP status codes.
//
// Usage:
//   node scripts/durability_suite.js              # run all cases
//   node scripts/durability_suite.js --only=wal_only_replay,crash_after_flush
//   BLACKBOX_WAL_FLUSH_BYTES=4096 node scripts/durability_suite.js
//
// Exit code: 0 if every HARD case passed, 1 otherwise. "characterization"
// cases never fail the run; they measure a known behaviour (e.g. the window in
// which an acknowledged write can be lost) and print it.

const fs = require("fs");
const os = require("os");
const path = require("path");
const http = require("http");
const { spawn } = require("child_process");

const ROOT = path.resolve(__dirname, "..");
const isWin = process.platform === "win32";
const BIN = path.join(ROOT, "build", isWin ? "Release" : "", isWin ? "BlackBox.exe" : "BlackBox");
const HOST = "127.0.0.1";
const PORT = 8080;

const onlyArg = (process.argv.find((a) => a.startsWith("--only=")) || "").slice("--only=".length);
const ONLY = onlyArg ? new Set(onlyArg.split(",").map((s) => s.trim()).filter(Boolean)) : null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- HTTP -------------------------------------------------------------------
function request(method, p, body) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? null : JSON.stringify(body);
    const req = http.request(
      { host: HOST, port: PORT, path: p, method,
        headers: { "Content-Type": "application/json", ...(data ? { "Content-Length": Buffer.byteLength(data) } : {}) } },
      (res) => {
        let buf = "";
        res.on("data", (c) => (buf += c));
        res.on("end", () => {
          let json = null;
          try { json = buf ? JSON.parse(buf) : null; } catch { /* leave raw */ }
          resolve({ status: res.statusCode, json, raw: buf });
        });
      }
    );
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

// --- Server lifecycle -------------------------------------------------------
let seq = 0;
function startServer(cwd, env) {
  const proc = spawn(BIN, [], {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", "ignore", process.env.DEBUG_SERVER === "1" ? "inherit" : "ignore"],
  });
  proc._seq = ++seq;
  return proc;
}
async function waitHealthy(timeoutMs = 10000) {
  const start = Date.now();
  let last = null;
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await request("GET", "/v1/health");
      if (r.status === 200) return;
      last = `status=${r.status}`;
    } catch (e) { last = e.code || e.message; }
    await sleep(100);
  }
  throw new Error(`server not healthy in ${timeoutMs}ms (last: ${last})`);
}
function waitExit(proc, timeoutMs = 5000) {
  return new Promise((resolve) => {
    if (proc.exitCode !== null || proc.signalCode !== null) return resolve();
    const t = setTimeout(resolve, timeoutMs);
    proc.once("exit", () => { clearTimeout(t); resolve(); });
  });
}
// Graceful-ish stop: the server has no SIGTERM handler, so this is really a
// terminate. Data survives only if already flushed (snapshot or bg flush).
async function stop(proc) {
  if (!proc) return;
  proc.kill("SIGTERM");
  await waitExit(proc);
}
// Hard crash: no chance to flush anything still in userspace buffers.
async function crash(proc) {
  if (!proc) return;
  proc.kill("SIGKILL");
  await waitExit(proc);
}

// --- Test scaffolding -------------------------------------------------------
function makeCtx(name) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `bbdur_${name}_`));
  return { name, dir, dataDir: path.join(dir, "data"), checks: [] };
}
function check(ctx, ok, msg, detail) {
  ctx.checks.push({ ok: !!ok, msg, detail });
  if (!ok) console.log(`    ✗ ${msg}${detail ? ` -- ${detail}` : ""}`);
  return ok;
}
function walPath(ctx, index) { return path.join(ctx.dataDir, `${index}.wal`); }

async function createIndex(name, fields, extra = {}) {
  return request("POST", "/v1/indexes", { name, schema: { fields, ...extra } });
}
async function putDoc(index, doc) {
  const r = await request("POST", `/v1/${index}/doc`, doc);
  return { status: r.status, id: r.json?.data?.id };
}
async function getDoc(index, id) {
  const r = await request("GET", `/v1/${index}/doc/${id}`);
  return { status: r.status, doc: r.json?.data?.doc };
}
async function snapshot() { return request("POST", "/v1/snapshot"); }

// ---------------------------------------------------------------------------
// CASES
// ---------------------------------------------------------------------------
const CASES = {};
function defcase(name, kind, fn) { CASES[name] = { name, kind, fn }; }

// 1. Clean restart preserves *content*, not just presence.
defcase("clean_restart_integrity", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { title: "text", body: "text", version: { type: "number", searchable: false } });
  const { id } = await putDoc("t", { title: "durable", body: "original", version: 1 });
  await request("PATCH", `/v1/t/doc/${id}`, { body: "patched body", version: 2 });
  await snapshot();
  await crash(p);
  p = startServer(ctx.dir, env); await waitHealthy();
  const got = await getDoc("t", id);
  check(ctx, got.status === 200, "doc present after restart", `status=${got.status}`);
  check(ctx, got.doc?.body === "patched body", "patched body persisted", `body=${JSON.stringify(got.doc?.body)}`);
  check(ctx, got.doc?.version === 2, "patched version persisted", `version=${JSON.stringify(got.doc?.version)}`);
  await crash(p);
});

// 2. WAL-only replay: no snapshot taken, data must come back from the WAL.
defcase("wal_only_replay", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const ids = [];
  for (let i = 0; i < 20; i++) ids.push((await putDoc("t", { body: `replay-doc-${i}` })).id);
  await sleep(400); // let the background maintenance flush persist the WAL
  await crash(p); // NOTE: no snapshot -> pure WAL replay path
  p = startServer(ctx.dir, env); await waitHealthy();
  let survived = 0;
  for (let i = 0; i < ids.length; i++) {
    const g = await getDoc("t", ids[i]);
    if (g.status === 200 && g.doc?.body === `replay-doc-${i}`) survived++;
  }
  check(ctx, survived === ids.length, "all flushed docs replayed from WAL", `${survived}/${ids.length}`);
  await crash(p);
});

// 3. Once flushed, data must survive a hard crash (SIGKILL preserves page cache).
defcase("crash_after_flush", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const ids = [];
  for (let i = 0; i < 10; i++) ids.push((await putDoc("t", { body: `flushed-${i}` })).id);
  await sleep(500); // exceed flush interval so the maintenance thread persists
  await crash(p); // hard kill; flushed bytes are in OS cache and must survive
  p = startServer(ctx.dir, env); await waitHealthy();
  let survived = 0;
  for (let i = 0; i < ids.length; i++) if ((await getDoc("t", ids[i])).status === 200) survived++;
  check(ctx, survived === ids.length, "flushed docs survive hard crash", `${survived}/${ids.length}`);
  await crash(p);
});

// 4. CHARACTERIZATION: how many acknowledged writes are lost if we crash
//    immediately after the 201? Measures the pre-durability ack window.
defcase("ack_durability_window", "characterization", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const N = 25;
  const ids = [];
  for (let i = 0; i < N; i++) {
    const r = await putDoc("t", { body: `canary-${i}` });
    if (r.status === 201) ids.push(i, r.id); // pair (i,id)
  }
  const acked = [];
  for (let i = 0; i < ids.length; i += 2) acked.push({ i: ids[i], id: ids[i + 1] });
  await crash(p); // crash the instant the last write was acknowledged
  p = startServer(ctx.dir, env); await waitHealthy();
  let lost = 0;
  for (const { id } of acked) if ((await getDoc("t", id)).status !== 200) lost++;
  check(ctx, true, `acknowledged writes: ${acked.length}, lost on immediate crash: ${lost}`,
    lost > 0 ? "acknowledged-but-not-durable window present" : "no loss observed");
  ctx.metric = { acked: acked.length, lost };
  await crash(p);
});

// 5. Appending garbage to the WAL tail must not destroy earlier good records.
defcase("wal_tail_corruption_survival", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const ids = [];
  for (let i = 0; i < 15; i++) ids.push((await putDoc("t", { body: `good-${i}` })).id);
  await snapshot(); // ensure the 15 good records are durably in the WAL/segment
  await crash(p);
  fs.appendFileSync(walPath(ctx, "t"), Buffer.from("CORRUPTTAILGARBAGE\x00\xff\x01\x02"));
  p = startServer(ctx.dir, env);
  let healthy = true;
  try { await waitHealthy(); } catch { healthy = false; }
  check(ctx, healthy, "server starts despite corrupt WAL tail");
  if (healthy) {
    let survived = 0;
    for (let i = 0; i < ids.length; i++) if ((await getDoc("t", ids[i])).status === 200) survived++;
    check(ctx, survived === ids.length, "all good records survive tail corruption", `${survived}/${ids.length}`);
    // And the DB must still accept new writes afterwards.
    const w = await putDoc("t", { body: "post-corruption-write" });
    check(ctx, w.status === 201 && (await getDoc("t", w.id)).status === 200,
      "writable after tail corruption", `status=${w.status}`);
  }
  await crash(p);
});

// 6. A torn final record (truncated write) must not lose the good prefix.
defcase("wal_torn_final_record", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const ids = [];
  for (let i = 0; i < 15; i++) ids.push((await putDoc("t", { body: `torn-${i}` })).id);
  await snapshot();
  await crash(p);
  // Simulate a partially-written final record by chopping bytes off the tail.
  const wp = walPath(ctx, "t");
  const size = fs.statSync(wp).size;
  fs.truncateSync(wp, Math.max(0, size - 7));
  p = startServer(ctx.dir, env);
  let healthy = true;
  try { await waitHealthy(); } catch { healthy = false; }
  check(ctx, healthy, "server starts with torn final record");
  if (healthy) {
    // The good prefix (persisted via snapshot) must remain intact.
    let survived = 0;
    for (let i = 0; i < ids.length; i++) if ((await getDoc("t", ids[i])).status === 200) survived++;
    check(ctx, survived === ids.length, "good prefix survives torn tail", `${survived}/${ids.length}`);
  }
  await crash(p);
});

// 7. Deletes must be durable (no tombstone resurrection) via both paths.
defcase("delete_durability", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const keep = (await putDoc("t", { body: "keep-me" })).id;
  const del = (await putDoc("t", { body: "delete-me" })).id;
  const delRes = await request("DELETE", `/v1/t/doc/${del}`);
  check(ctx, delRes.status === 200 || delRes.status === 204, "delete acknowledged", `status=${delRes.status}`);
  await snapshot();
  await crash(p);
  p = startServer(ctx.dir, env); await waitHealthy();
  check(ctx, (await getDoc("t", del)).status === 404, "deleted doc stays deleted after restart");
  check(ctx, (await getDoc("t", keep)).status === 200, "sibling doc still present");
  const s = await request("GET", `/v1/t/search?q=delete-me&mode=bm25`);
  check(ctx, (s.json?.data?.hits?.length ?? 0) === 0, "deleted doc absent from search after restart");
  await crash(p);
});

// 8. Concurrent writes must all be durable across a restart.
defcase("concurrent_writes_durability", "hard", async (ctx, env) => {
  let p = startServer(ctx.dir, env); await waitHealthy();
  await createIndex("t", { body: "text" });
  const N = 200;
  const results = await Promise.all(
    Array.from({ length: N }, (_, i) => putDoc("t", { body: `concurrent-${i}`, n: i }))
  );
  const ids = results.filter((r) => r.status === 201).map((r) => r.id);
  check(ctx, ids.length === N, "all concurrent writes acknowledged", `${ids.length}/${N}`);
  await sleep(500); // allow background flush
  await crash(p);
  p = startServer(ctx.dir, env); await waitHealthy();
  let survived = 0;
  for (const id of ids) if ((await getDoc("t", id)).status === 200) survived++;
  check(ctx, survived === ids.length, "all acknowledged concurrent writes durable", `${survived}/${ids.length}`);
  await crash(p);
});

// ---------------------------------------------------------------------------
async function main() {
  if (!fs.existsSync(BIN)) {
    console.error(`BlackBox binary not found at ${BIN}. Build first: cmake --build build`);
    process.exit(2);
  }
  const env = {};
  for (const k of ["BLACKBOX_WAL_FLUSH_BYTES", "BLACKBOX_WAL_FLUSH_MS", "BLACKBOX_WAL_FSYNC"])
    if (process.env[k]) env[k] = process.env[k];

  const names = Object.keys(CASES).filter((n) => !ONLY || ONLY.has(n));
  const report = { timestamp: new Date().toISOString(), binary: BIN, env, cases: [] };
  let hardFailures = 0;

  const progressPath = path.join(__dirname, "durability_suite_progress.log");
  const log = (line) => { console.log(line); try { fs.appendFileSync(progressPath, line + "\n"); } catch {} };
  try { fs.writeFileSync(progressPath, ""); } catch {}

  log(`\nBlackBox durability suite -- ${names.length} case(s)\n`);
  for (const name of names) {
    const c = CASES[name];
    const ctx = makeCtx(name);
    log(`> ${name} [${c.kind}]`);
    let error = null;
    try { await c.fn(ctx, env); }
    catch (e) { error = e.message; check(ctx, false, "case threw", e.message); }
    finally { try { fs.rmSync(ctx.dir, { recursive: true, force: true }); } catch {} }

    const failed = ctx.checks.filter((k) => !k.ok);
    const passed = c.kind === "characterization" ? true : failed.length === 0 && !error;
    if (!passed) hardFailures++;
    const status = c.kind === "characterization" ? "MEASURED" : passed ? "PASS" : "FAIL";
    for (const k of failed) log(`    x ${k.msg}${k.detail ? ` -- ${k.detail}` : ""}`);
    if (ctx.metric) log(`    metric: ${JSON.stringify(ctx.metric)}`);
    log(`  ${status} (${ctx.checks.length - failed.length}/${ctx.checks.length} checks)\n`);
    report.cases.push({ name, kind: c.kind, status, error,
      checks: ctx.checks, metric: ctx.metric || null });
    // Persist full report after every case so partial runs are still inspectable.
    try { fs.writeFileSync(path.join(__dirname, "durability_suite_results.json"), JSON.stringify(report, null, 2)); } catch {}
  }

  log("------------------------------------------------------------");
  for (const c of report.cases) log(`  ${c.status.padEnd(9)} ${c.name}`);
  log("------------------------------------------------------------");
  log(hardFailures === 0 ? "ALL HARD DURABILITY CASES PASSED"
                         : `${hardFailures} HARD CASE(S) FAILED`);
  log("SUITE_COMPLETE");
  process.exitCode = hardFailures === 0 ? 0 : 1;
}

process.on("unhandledRejection", (e) => { console.error("unhandledRejection:", e); process.exit(3); });
main();
