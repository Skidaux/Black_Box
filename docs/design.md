# BlackBox Design Outline

## API Standards
- Version all endpoints under `/v1/`.
- JSON envelope: `{"status":"ok","data":{...}}` or `{"status":"error","error":{"code","message"}}`; no HTML responses.
- Enforce `Content-Type: application/json` for write endpoints; reject oversize bodies.
- Endpoints (initial set):
  - `GET /v1/health` — status + counts.
  - `POST /v1/index` — index a JSON document, return `{id}`.
  - `GET /v1/doc/{id}` — fetch a document, 404 if missing.
  - `DELETE /v1/doc/{id}` — tombstone/delete.
  - `GET /v1/search?q=...&from=0&size=10` — paginated hits and total.
- Pagination guardrails: clamp `from`/`size` to sane limits; default `size=10`.
- Observability: include request IDs in logs; basic counters for requests/errors/latency (to be added).

## Storage Direction
- Append-only segment log for durability and crash recovery.
  - Records: `[length][payload][checksum]` where payload is a JSON blob (`{"id":N,"doc":...}` or tombstone).
  - Checksums: per-record CRC32/xxHash; footer holds segment length + checksum for quick integrity checks.
- Manifest file (`data/manifest.json`) tracks active segments, next ID, and settings; fsync updates after writing.
- Immutable inverted-index segment (`segment-<n>.idx`) built from flushed log; contains term dictionary + postings (sorted doc IDs).
- Tombstones recorded in log; applied at query time and cleared during merge.
- Background merge compacts multiple segments into a new one to keep posting lists short and reclaim deletes.
- Memory-mapped reads for segments to avoid copies; small in-memory caches for hot terms/doc offsets.
- Crash safety: append + fsync log and manifest; detect partial records via checksum/length.

## Indexing Semantics
- Tokenizer: lowercase alnum tokens; configurable stopwords/stemming later.
- Index strings recursively across objects/arrays; optional field-name indexing for scoped queries later.
- Postings: term → sorted doc IDs; AND queries use intersections; later add OR/phrase and simple ranking.
- Schemas can now declare `doc_id` (e.g. `{"field":"sku","type":"string"}`) to make a field the authoritative document identifier. IDs remain unique within the index and can be fetched/updated/deleted using either auto-increment IDs or the custom IDs.
- Optional `relation` config (e.g. `{"field":"parent","target_index":"orders"}`) allows documents to reference other docs in the same or external indexes. Search requests may ask for inline relation embedding (`include_relations=inline`) or grouped hierarchical responses (`include_relations=hierarchy`) with an adjustable `max_relation_depth`.
- `image` field type stores raw binary payloads (PNG/ICO/etc.) with `max_kb` enforcement; WAL/snapshot encode images separately to avoid base64 bloat.
- Custom aggregation APIs let users compose multi-index views (e.g., page→site→favicon) with declarative projections and relation trees while reusing all search algorithms and filter knobs.

## Near-Term Implementation Steps
- Keep current in-memory index but wrap API with `/v1` and envelopes.
- Add delete/tombstone support in-memory to mirror future on-disk flow.
- Introduce persistence shell: manifest writer + append-only log writer with checksums; load on startup.
- Add metrics/logging hooks to trace requests and errors.

## Code Layout
- `src/core/Analyzer.cpp` / `include/minielastic/Analyzer.hpp`: tokenization/analyzers.
- `src/storage/LogStore.cpp` / `include/minielastic/LogStore.hpp`: append-only log with per-record checksums.
- `src/engine/BlackBox.cpp` / `include/BlackBox.hpp`: in-memory index + persistence wiring.
- `src/server/BlackBoxHttpServer.cpp` / `include/BlackBoxHttpServer.hpp`: HTTP API surface.
- `src/main.cpp`: entry point wiring server + data directory.
