# BlackBox DB - In development

---

**BlackBox** is a document database in C++ and inspired by [Elastic](https://www.elastic.co/elasticsearch). It combines full- ext, vector, and hybrid search so you can index structured or semi-structured data and query it through a simple HTTP API.

## Capabilities

- **Flexible schema** — define index schemas with text, numeric, boolean, array, and vector fields plus optional document IDs and cross-index relations.
- **Rich document lifecycle** — create, fetch, update (PUT/PATCH), and delete documents via REST while the server handles WAL persistence, segmenting, and snapshots/load.
- **Search modes** — run BM25/lexical, fuzzy, hybrid, or pure vector queries with filters, relation expansion (inline/hierarchy), pagination, and weighting controls.
- **Custom aggregations** — create pipelines that hop across relations (e.g., page→site→favicon) and return exactly the fields you need. (Like GraphQK)
- **Custom tuning** — tune background flushing, merge frequency, compression, and vector clustering through environment variables.

Checkout `docs/documentation.md` for the API usage docs/examples

## Building from Source

BlackBox uses CMake and C++17. The `CMakeLists.txt` creates the main `BlackBox` binary including `BlackBoxTests`.

### Linux (GCC/Clang)

1. Install dependencies: `sudo apt install build-essential cmake` (or the equivalents for your distro).
2. Configure the project: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release`.
3. Build the binaries: `cmake --build build`.
4. (Optional) Run tests: `ctest --test-dir build`.

### Windows (Visual Studio or MinGW)

1. Install the [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/) with the “Desktop development with C++” workload, or install [MSYS2/MinGW-w64](https://www.msys2.org/) plus CMake.
2. Generate a build:
   - Visual Studio: `cmake -S . -B build -G "Visual Studio 17 2022" -A x64`.
   - MinGW: run from an MSYS2/MinGW shell, `cmake -S . -B build -G "MinGW Makefiles"`.
3. Build:
   - Visual Studio: `cmake --build build --config Release`.
   - MinGW: `cmake --build build`.

The compiled executables are in `build/` (`build/Release/` when using Visual Studio multi-config generators). Point the runtime at your data/configuration, then start the Database by executing the `BlackBox` binary.
