# BlackBox HTTP API & Usage

Base URL: `http://127.0.0.1:8080`

## Health & Metrics
- `GET /v1/health` — returns `{status:"ok"}` when the server is up.
- `GET /v1/metrics` — JSON with uptime, config, totals, and per-index stats (docs, segments, vectors, WAL bytes, pending ops).

## Index Management
- `POST /v1/indexes` — create an index. Body:
```json
{
  "name": "demo",
  "schema": {
    "fields": {
      "title": "text",
      "body": "text",
      "tags": "array",
      "labels": "array",
      "flag": "bool",
      "priority": "number",
      "vec": { "type": "vector", "dim": 3 }
    }
  }
}
```
- `GET /v1/indexes` — list indexes with basic stats.
- Snapshots:
  - `POST /v1/snapshot` (optional `?path=...`) — write manifest/snapshots.
  - `POST /v1/snapshot/load` (optional `?path=...`) — load a manifest/snapshots.

## Documents
- `POST /v1/{index}/doc` — index a document (Content-Type: application/json). Returns `id`.
- `GET /v1/{index}/doc/{id}` — fetch a document.
- `PUT /v1/{index}/doc/{id}` — replace a document.
- `PATCH /v1/{index}/doc/{id}` — partial update (merge fields).
- `DELETE /v1/{index}/doc/{id}` — delete a document.

## Search
- `GET /v1/{index}/search`
  - Common params: `q` (query text), `mode` (`bm25`, `lexical`, `fuzzy`, `semantic`, `hybrid`, `vector`), `from`, `size`.
  - Fuzzy: `distance` (max edit distance).
  - Hybrid: `w_bm25`, `w_semantic`, `w_lexical` weights.
  - Vector: `mode=vector`, `vec=comma,separated,floats` (provide `q` placeholder if required by clients).
  - Filters:
    - `tag`, `label`, `flag` (bool) shortcuts.
    - Per-field doc-value filters: `filter_<field>=value` (array/string/bool/number), `filter_<field>_min`, `filter_<field>_max` for numeric ranges.

Response shape (example):
```json
{
  "status": "ok",
  "data": {
    "query": "quick fox",
    "mode": "bm25",
    "from": 0,
    "size": 10,
    "total": 2,
    "hits": [
      { "id": 1, "score": 1.23, "doc": { "title": "...", "body": "...", "...": "..." } }
    ]
  }
}
```

## Configuration / Tunables
Environment variables:
- `BLACKBOX_FLUSH_DOCS` — number of ops before forcing WAL→segment flush (default 5000).
- `BLACKBOX_MERGE_SEGMENTS` — max segments before auto-merge into one (default 10).
- `BLACKBOX_COMPRESS` — `1/true/on` to compress snapshot sections (default on).
- `BLACKBOX_ANN_CLUSTERS` — default coarse vector clusters (default 8).

## Notes for Library Authors
- All endpoints return JSON with `{status:"ok",data:...}` or `{status:"error",error:{code,message}}`.
- Respect Content-Type `application/json` on write endpoints.
- For vector search, always supply `mode=vector` and `vec` as comma-separated floats.
- Use `/v1/metrics` to monitor uptime, backlogs (pending_ops), and WAL size.
