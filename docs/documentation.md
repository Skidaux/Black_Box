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
- `POST /v1/{index}/doc` — index a document (Content-Type: application/json). Returns `id` and, when configured, `doc_id` (custom identifier derived from schema). Image fields use `{ "content": "<base64>", "format": "png", "encoding": "base64" }`; the server stores raw binary and enforces `max_kb` limits.
- `GET /v1/{index}/doc/{id}` — fetch a document by auto-increment ID or custom ID.
- `PUT /v1/{index}/doc/{id}` — replace a document (accepts either ID form).
- `PATCH /v1/{index}/doc/{id}` — partial update (merge fields).
- `DELETE /v1/{index}/doc/{id}` — delete a document.

Schema extensions:
- `doc_id`: `{ "field": "sku", "type": "string", "enforce_unique": true }` — designates the field that becomes the public document ID.
- `relation`: `{ "field": "parent", "target_index": "orders", "allow_cross_index": true }` — stores a relation reference (object `{ "id": "...", "index": "..." }` or shorthand string/number).

## Search
- `GET /v1/{index}/search`
  - Common params: `q` (query text), `mode` (`bm25`, `lexical`, `fuzzy`, `semantic`, `hybrid`, `vector`), `from`, `size`.
  - Fuzzy: `distance` (max edit distance).
  - Hybrid: `w_bm25`, `w_semantic`, `w_lexical` weights.
  - Vector: `mode=vector`, `vec=comma,separated,floats` (provide `q` placeholder if required by clients).
  - Relations: `include_relations=inline|hierarchy|none` (default `none`) and `max_relation_depth` (default `1`).  
    - `inline` embeds the linked document under each hit.  
    - `hierarchy` groups hits beneath their shared relation reference and adds `relation_groups` to the response.
  - Custom aggregations (see below) inherit the same parameters and can embed multi-hop relation chains (e.g., page→site→favicon).
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
      {
        "id": 1,
        "doc_id": "sku-1",
        "score": 1.23,
        "doc": { "title": "...", "body": "...", "...": "..." },
        "relation": { "index": "demo", "id": "sku-parent", "doc": { ... } }
      }
    ],
    "relation_mode": "inline",
    "relation_groups": [
      { "relation_ref": { "index": "demo", "id": "sku-parent" }, "children": [ ... ] }
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

## Custom Aggregation APIs
- `GET /v1/custom` — list registered aggregation templates.
- `GET /v1/custom/{name}` — fetch template definition.
- `PUT /v1/custom/{name}` — create/update a template. Body example:
```json
{
  "base_index": "webpages",
  "select": ["title", "snippet", "site_ref"],
  "relations": [
    {
      "name": "site",
      "field": "site_ref",
      "target_index": "sites",
      "select": ["domain", "description"],
      "relations": [
        {
          "name": "favicon",
          "field": "favicon_ref",
          "target_index": "favicons",
          "select": ["format", "bytes"],
          "include_image": true,
          "image_field": "image"
        }
      ]
    }
  ]
}
```
- `DELETE /v1/custom/{name}` — remove template.
- `POST /v1/custom/{name}` — execute template. Body accepts the same search parameters as `/v1/{index}/search` (e.g., `q`, `mode`, `from`, `size`, `vec`, `distance`, `w_bm25`, etc.).
- Responses mirror normal search hits but follow the template’s projection and nested relation definitions.

## Notes for Library Authors
- All endpoints return JSON with `{status:"ok",data:...}` or `{status:"error",error:{code,message}}`.
- Respect Content-Type `application/json` on write endpoints.
- For vector search, always supply `mode=vector` and `vec` as comma-separated floats.
- Use `/v1/metrics` to monitor uptime, backlogs (pending_ops), and WAL size.
