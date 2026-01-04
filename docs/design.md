# BlackBox Design Outline

## API Standards

- Version all endpoints under `/v1/`; responses use envelopes `{"status":"ok","data":...}` or `{"status":"error","error":{"code","message"}}`.
- Enforce `Content-Type: application/json` for write endpoints; reject oversize bodies (configurable via `BLACKBOX_MAX_BODY_KB`).
- Current endpoints:
  - Health/config: `GET /v1/health`, `GET /v1/metrics`, `GET /metrics` (Prom), `GET /v1/config`.
  - Index management: `POST /v1/indexes`, `GET /v1/indexes`, snapshot save/load.
  - Documents: `POST /v1/{index}/doc`, bulk `POST /v1/{index}/_bulk`, `GET/PUT/PATCH/DELETE /v1/{index}/doc/{id}`.
  - Search: `GET /v1/{index}/search` with hybrid/vector/fuzzy + filters (404 on missing index).
  - Custom aggregations: CRUD + execute under `/v1/custom`.
- Pagination guardrails: clamp `from`/`size` to sane limits; default `size=10`.
- Observability: add request IDs and per-endpoint counters/latency histograms (still to be built out).

## Storage Direction

- Append-only WAL for durability and crash recovery with per-record CRC32.
- Manifest file (`data/index.manifest`) tracks active segments, next ID, and settings; writes are fsynced before old segments are deleted. Segment flush triggers include op count, time (`BLACKBOX_FLUSH_MS`), and WAL growth (`BLACKBOX_FLUSH_WAL_BYTES`) to bound in-memory backlog; bulk endpoints can 429 when backpressure is active.
- Segment files (`*.skd`) contain docs, postings, doc-values, vectors, images, ANN metadata (centroids + HNSW graph with M/ef_search), and tombstones; sections are checksummed and optionally compressed.
- Schema sidecars (`<index>.schema.json`) are persisted so WAL-only recovery retains field settings (doc_id/relation/vector/image).
- Tombstones recorded in WAL and segment tombstone lists; applied at query time and cleared during merge.
- Background merge compacts multiple segments into one to keep posting lists short and reclaim deletes.
- Crash safety: WAL supports fsync-on-flush, manifest/segments are fsynced on write; WAL truncates on checksum failure.

## Indexing Semantics

- Tokenizer: lowercase alnum tokens; configurable stopwords/stemming later.
- Index strings recursively across objects/arrays; optional field-name indexing for scoped queries later.
- Postings: term → sorted doc IDs; AND queries intersect; OR/phrase planned.
- Schemas can declare `doc_id` (e.g., `{"field":"sku","type":"string","enforce_unique":true}`) to expose a stable document identifier. Operations accept auto IDs or custom IDs.
- Optional `relation` config (`{"field":"parent","target_index":"orders","allow_cross_index":false}`) lets documents reference other docs; search can inline or hierarchy-group relations with depth limits.
- `image` field type stores raw binary payloads (PNG/ICO/etc.) with `max_kb` enforcement; WAL/snapshot encode images separately to avoid base64 bloat.
- Custom aggregation APIs compose multi-index views (e.g., page→site→favicon) with declarative projections and relation trees while reusing search/filter knobs.

## Near-Term Implementation Steps

- Extend queries: OR/phrase search, safer fuzzy expansion, stronger ANN (e.g., HNSW), richer doc-value filters.
- Ops hardening: request IDs, structured logs, per-endpoint metrics, rate/body-size guards.
- Resource controls: add time/size-based flush triggers, backpressure on bulk ingest, and eviction/caching for segment reads.
- Tooling: durability/chaos tests around WAL flush + manifest rewrite; benchmark pipelines.
