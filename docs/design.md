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
- Observability: request IDs on core routes; Prometheus `/metrics` exports uptime, docs/segments/vectors/WAL bytes, pending ops, cache stats (snapshot/doc), replay errors, ANN recall counters; per-index metrics included.
- Merge queue: flush triggers enqueue merges; maintenance thread processes merge jobs with size-aware throttling and optional MBps cap instead of blocking inline.

## Storage Direction

- Append-only WAL for durability and crash recovery with per-record CRC32. WAL now carries a `BBWAL` magic + versioned header (with `schema_id`) and versioned records that include `op_id` so readers can validate format and ordering; legacy WALs remain readable.
- Manifest file (`data/index.manifest`) is versioned (`format=blackbox_manifest`, `version=2`) and records active segments, `next_id`, `next_op_id`, `schema_id`/`schema_version`, and settings; writes are fsynced before old segments are deleted. Segment flush triggers include op count, time (`BLACKBOX_FLUSH_MS`), and WAL growth (`BLACKBOX_FLUSH_WAL_BYTES`) to bound in-memory backlog; bulk endpoints can 429 when backpressure is active.
- WAL migration: legacy WALs are auto-rewritten to the versioned format (backup as `.legacy`) on open when a schema_id is available; schema_id mismatches are logged and surfaced via stats for operational visibility. Loading legacy manifests triggers an automatic rewrite to the latest manifest format.
- Segment files (`*.skd`) contain docs, postings, doc-values, vectors, images, ANN metadata (centroids + HNSW graph with M/ef_search), and tombstones; sections are checksummed and optionally compressed.
- Schema sidecars (`<index>.schema.json`) are persisted so WAL-only recovery retains field settings (doc_id/relation/vector/image).
- Tombstones recorded in WAL and segment tombstone lists; applied at query time and cleared during merge.
- Background merge compacts multiple segments into one to keep posting lists short and reclaim deletes; merge backpressure uses size-aware sleep with optional throughput cap (`BLACKBOX_MERGE_MBPS`) and budget/throttle settings.
- Crash safety: WAL supports fsync-on-flush, manifest/segments are fsynced on write; WAL truncates on checksum failure.

## Indexing Semantics

- Tokenizer: lowercase alnum tokens; configurable stopwords/stemming later.
- Index strings recursively across objects/arrays; optional field-name indexing for scoped queries later.
- Postings: term → sorted doc IDs; AND queries intersect; OR/phrase planned.
- Schemas can declare `doc_id` (e.g., `{"field":"sku","type":"string","enforce_unique":true}`) to expose a stable document identifier. Operations accept auto IDs or custom IDs.
- Optional `relation` config (`{"field":"parent","target_index":"orders","allow_cross_index":false}`) lets documents reference other docs; search can inline or hierarchy-group relations with depth limits.
- Optional non-searchable fields (`searchable:false`) are stored but excluded from indexes; a stored-field scan endpoint exists for maintenance/migrations. A `query_values` field type stores arrays of `{query, score}` pairs (score 0..1) that can be stored-only or boost search when `searchable:true`.
- `image` field type stores raw binary payloads (PNG/ICO/etc.) with `max_kb` enforcement; WAL/snapshot encode images separately to avoid base64 bloat.
- Custom aggregation APIs compose multi-index views (e.g., page→site→favicon) with declarative projections and relation trees while reusing search/filter knobs.
- Caching: snapshot chunk cache and document cache use LRU with entry/MB caps; hit/miss/bytes surfaced via config and Prometheus.
- Rate/body guards: global and per-index QPS/body limits on search and writes return 429s when exceeded.
- Shipping: `/v1/ship` exports shipping plan; `/v1/ship/apply` and `/v1/ship/fetch_apply` validate manifest/segment formats then load snapshots (basis for future WAL shipping).

## Remaining Work

- Segment reader cache for postings/doc-values with memory budget and hit metrics.
- Merge IO scheduler/backpressure beyond sleep-based throttling.
- Broader structured logging and per-index quotas on all routes.
- WAL/shipping streaming for replicas; shard/replica coordination metadata.
- Additional chaos/benchmark coverage for shipping and recovery paths.
