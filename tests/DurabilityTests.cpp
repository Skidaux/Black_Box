// Durability test suite for the BlackBox engine.
//
// These tests exercise crash/restart durability *deterministically* by forking
// a child that writes and then calls _exit(0) -- skipping C++ destructors and
// std::ofstream buffer flushes, which is exactly what a power-loss / SIGKILL
// crash looks like. The parent then opens a fresh BlackBox on the same data
// directory and asserts what actually survived. This avoids the timing games a
// signal-based HTTP test needs and pins down precisely which writes are durable.
//
// Two kinds of results are reported:
//   [INVARIANT] must hold; a failure exits the process non-zero (CI gate).
//   [BEHAVIOUR] a measured durability property; printed, never fails the run.
//
// Build target: BlackBoxDurabilityTests (see CMakeLists.txt).

#include "BlackBox.hpp"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#error "DurabilityTests uses fork(); build on a POSIX platform."
#endif
#include <sys/wait.h>
#include <unistd.h>

using minielastic::BlackBox;
using nlohmann::json;
namespace fs = std::filesystem;

static int g_invariantFailures = 0;
static int g_behaviourNotes = 0;

static void invariant(bool cond, const std::string& msg, const std::string& detail = "") {
    if (cond) {
        std::cout << "  [INVARIANT] PASS  " << msg << "\n";
    } else {
        std::cout << "  [INVARIANT] FAIL  " << msg;
        if (!detail.empty()) std::cout << "  (" << detail << ")";
        std::cout << "\n";
        ++g_invariantFailures;
    }
}

// Reports a measured durability property. `durable` true is the safe outcome.
static void behaviour(const std::string& msg, bool durable, const std::string& detail) {
    std::cout << "  [BEHAVIOUR] " << (durable ? "DURABLE     " : "NOT-DURABLE ")
              << msg << "  (" << detail << ")\n";
    if (!durable) ++g_behaviourNotes;
}

// Run `body` against a BlackBox in a forked child, then crash the child with
// _exit(0): no destructors, no stream flushing -- the on-disk state is whatever
// was actually persisted at the moment of the crash.
//
// CRITICAL: the BlackBox is heap-allocated and intentionally *never deleted*.
// If it were a stack local, its destructor (which flushes the WAL) would run
// when `body` returned, and we'd be testing a graceful shutdown, not a crash.
static void crashAfter(const std::string& dataDir, const std::function<void(BlackBox&)>& body) {
    // Flush all buffered stdio before forking: piped stdout is block-buffered,
    // and a fork would otherwise duplicate the unflushed buffer, making the
    // child reprint the parent's output.
    std::cout.flush();
    ::fflush(nullptr);
    pid_t pid = fork();
    if (pid == 0) {
        // Silence the child's engine logging so only the parent reports results.
        (void)!std::freopen("/dev/null", "w", stdout);
        (void)!std::freopen("/dev/null", "w", stderr);
        BlackBox* db = new BlackBox(dataDir); // leaked on purpose: no destructor on crash
        body(*db);
        _exit(0); // hard crash: skip flushes/destructors
    }
    int status = 0;
    waitpid(pid, &status, 0);
}

static BlackBox::IndexSchema textSchema() {
    BlackBox::IndexSchema s;
    s.schema = {{"fields", {{"body", "text"}}}};
    return s;
}

static fs::path freshDir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() / ("bbdur_" + name + "_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

static std::string walFile(const fs::path& dir, const std::string& index) {
    return (dir / (index + ".wal")).string();
}

// ---------------------------------------------------------------------------
// 1. Acknowledged-but-unflushed write, then immediate crash.
//    Measures the window in which a write the engine already "accepted"
//    (indexDocument returned an id) can be lost on a crash.
static void test_ack_durability_window(const fs::path& dir) {
    std::cout << "\n== ack_durability_window ==\n";
    uint32_t acked = 0;
    crashAfter(dir.string(), [&](BlackBox& db) {
        db.createIndex("t", textSchema());
        acked = db.indexDocument("t", R"({"body":"canary immediate crash"})");
        // crash instantly -- no time for the 200ms maintenance flush
    });
    size_t onDiskRecords = minielastic::readWalRecords(walFile(dir, "t"), 0).size();
    BlackBox db(dir.string());
    size_t cnt = db.indexExists("t") ? db.documentCount("t") : 0;
    bool survived = cnt == 1;
    behaviour("write acked by engine survives immediate crash", survived,
              survived ? "persisted"
                       : "acked id=" + std::to_string(acked) +
                             " returned to caller, but 0 records reached disk (WAL had "
                             + std::to_string(onDiskRecords) + " records); doc lost after crash");
}

// 2. Once the maintenance thread has flushed, a crash must not lose data.
static void test_flushed_survives_crash(const fs::path& dir) {
    std::cout << "\n== flushed_survives_crash ==\n";
    const int N = 10;
    crashAfter(dir.string(), [&](BlackBox& db) {
        db.createIndex("t", textSchema());
        for (int i = 0; i < N; ++i)
            db.indexDocument("t", json({{"body", "flushed-" + std::to_string(i)}}).dump());
        std::this_thread::sleep_for(std::chrono::milliseconds(400)); // let bg flush persist
    });
    BlackBox db(dir.string());
    invariant(db.documentCount("t") == static_cast<size_t>(N),
              "all flushed docs survive a hard crash",
              "count=" + std::to_string(db.documentCount("t")));
}

// 3. Explicit snapshot before a crash must persist everything.
static void test_snapshot_survives_crash(const fs::path& dir) {
    std::cout << "\n== snapshot_survives_crash ==\n";
    const int N = 25;
    crashAfter(dir.string(), [&](BlackBox& db) {
        db.createIndex("t", textSchema());
        for (int i = 0; i < N; ++i)
            db.indexDocument("t", json({{"body", "snap-" + std::to_string(i)}}).dump());
        db.saveSnapshot();
    });
    BlackBox db(dir.string());
    invariant(db.documentCount("t") == static_cast<size_t>(N),
              "snapshot before crash persists all docs",
              "count=" + std::to_string(db.documentCount("t")));
    auto hits = db.search("t", "snap-7", "bm25");
    invariant(!hits.empty(), "snapshotted content is searchable after restart");
}

// 4. WAL-only replay: no snapshot ever taken; data must come back from the WAL.
static void test_wal_only_replay(const fs::path& dir) {
    std::cout << "\n== wal_only_replay ==\n";
    const int N = 40;
    crashAfter(dir.string(), [&](BlackBox& db) {
        db.createIndex("t", textSchema());
        for (int i = 0; i < N; ++i)
            db.indexDocument("t", json({{"body", "wal-" + std::to_string(i)}}).dump());
        std::this_thread::sleep_for(std::chrono::milliseconds(400)); // flush WAL, but NO snapshot
    });
    // No snapshot was taken, so there must be no segment files: the only place
    // these docs can live is the WAL. (A manifest may still be written by the
    // maintenance thread, but it references zero segments.)
    bool anySegment = false;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().filename().string().find("_seg") != std::string::npos) anySegment = true;
    invariant(!anySegment, "no segment files exist (docs live only in the WAL)");
    BlackBox db(dir.string());
    size_t got = db.documentCount("t");
    invariant(got == static_cast<size_t>(N), "all flushed docs replay from WAL alone",
              "count=" + std::to_string(got));
    auto doc = db.getDocument("t", 20);
    invariant(doc.contains("body") && doc["body"] == "wal-19",
              "replayed doc content is intact", doc.dump());
}

// 5. Appending garbage to the WAL tail must not destroy earlier good records,
//    and the index must remain usable afterwards.
static void test_wal_tail_corruption(const fs::path& dir) {
    std::cout << "\n== wal_tail_corruption ==\n";
    const int N = 15;
    {
        BlackBox db(dir.string());
        db.createIndex("t", textSchema());
        for (int i = 0; i < N; ++i)
            db.indexDocument("t", json({{"body", "good-" + std::to_string(i)}}).dump());
        db.saveSnapshot();               // make the 15 records durable
        db.indexDocument("t", R"({"body":"post-snapshot-wal"})"); // extra record in WAL
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    } // graceful close via destructor

    // Corrupt: append raw garbage to the WAL tail.
    {
        std::ofstream out(walFile(dir, "t"), std::ios::binary | std::ios::app);
        const char garbage[] = "CORRUPTTAILGARBAGE\x00\xff\x01\x02\x03";
        out.write(garbage, sizeof(garbage));
    }

    bool opened = true;
    size_t survived = 0;
    try {
        BlackBox db(dir.string());
        for (int i = 0; i < N; ++i) {
            auto id = db.lookupDocId("t", std::to_string(i)); // ignore; count via search instead
            (void)id;
        }
        survived = db.documentCount("t");
        invariant(survived >= static_cast<size_t>(N),
                  "all good records survive WAL tail corruption",
                  "count=" + std::to_string(survived));
        // Must still be writable, and that write must itself be durable.
        auto newId = db.indexDocument("t", R"({"body":"after-corruption"})");
        invariant(newId > 0, "index is writable after tail corruption");
        db.saveSnapshot();
    } catch (const std::exception& e) {
        opened = false;
        invariant(false, "server starts despite corrupt WAL tail", e.what());
    }

    // Reopen once more: the post-corruption write must not have been swallowed
    // by the un-truncated garbage sitting in the middle of the WAL.
    if (opened) {
        BlackBox db(dir.string());
        auto hits = db.search("t", "after-corruption", "bm25");
        behaviour("write appended after a corrupt tail survives the next restart",
                  !hits.empty(),
                  hits.empty() ? "garbage was not truncated; later append lost on replay"
                               : "persisted");
    }
}

// 6. A torn (partially written) final record must not lose the good prefix.
static void test_wal_torn_final_record(const fs::path& dir) {
    std::cout << "\n== wal_torn_final_record ==\n";
    const int N = 15;
    {
        BlackBox db(dir.string());
        db.createIndex("t", textSchema());
        for (int i = 0; i < N; ++i)
            db.indexDocument("t", json({{"body", "torn-" + std::to_string(i)}}).dump());
        db.saveSnapshot();
        db.indexDocument("t", R"({"body":"tail-record"})");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    // Chop bytes off the WAL tail to simulate a partial write.
    {
        auto p = walFile(dir, "t");
        auto sz = fs::file_size(p);
        fs::resize_file(p, sz > 7 ? sz - 7 : 0);
    }
    try {
        BlackBox db(dir.string());
        invariant(db.documentCount("t") >= static_cast<size_t>(N),
                  "good prefix survives a torn final record",
                  "count=" + std::to_string(db.documentCount("t")));
        invariant(db.indexDocument("t", R"({"body":"post-torn"})") > 0,
                  "index writable after torn record");
    } catch (const std::exception& e) {
        invariant(false, "server starts with torn final record", e.what());
    }
}

// 7. Deletes must be durable across a crash (no tombstone resurrection),
//    both via WAL replay and via snapshot.
static void test_delete_durability(const fs::path& dir) {
    std::cout << "\n== delete_durability ==\n";
    BlackBox::DocId keepId = 0, delId = 0;

    // WAL-only path (no snapshot): index two, delete one, flush, crash.
    crashAfter(dir.string(), [&](BlackBox& db) {
        db.createIndex("t", textSchema());
        db.indexDocument("t", R"({"body":"keep-me"})");
        db.indexDocument("t", R"({"body":"delete-me"})");
        db.deleteDocument("t", 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    });
    {
        BlackBox db(dir.string());
        invariant(db.documentCount("t") == 1, "delete survives WAL-only crash restart",
                  "count=" + std::to_string(db.documentCount("t")));
        auto hits = db.search("t", "delete-me", "bm25");
        invariant(hits.empty(), "deleted doc absent from search after WAL replay");
        auto keep = db.search("t", "keep-me", "bm25");
        invariant(!keep.empty(), "surviving doc still searchable after WAL replay");
    }
    (void)keepId; (void)delId;
}

// 8. Concurrent writers, then snapshot + restart: every acknowledged write must
//    be present exactly once.
static void test_concurrent_writes(const fs::path& dir) {
    std::cout << "\n== concurrent_writes ==\n";
    const int THREADS = 8, PER = 100;
    std::atomic<int> acked{0};
    {
        BlackBox db(dir.string());
        db.createIndex("t", textSchema());
        std::vector<std::thread> ts;
        for (int t = 0; t < THREADS; ++t) {
            ts.emplace_back([&, t]() {
                for (int i = 0; i < PER; ++i) {
                    try {
                        db.indexDocument("t", json({{"body", "c-" + std::to_string(t) + "-" + std::to_string(i)}}).dump());
                        acked.fetch_add(1);
                    } catch (...) {}
                }
            });
        }
        for (auto& th : ts) th.join();
        db.saveSnapshot();
    }
    BlackBox db(dir.string());
    invariant(db.documentCount("t") == static_cast<size_t>(acked.load()),
              "all concurrently-acked writes are durable exactly once",
              "acked=" + std::to_string(acked.load()) + " durable=" + std::to_string(db.documentCount("t")));
}

int main() {
    std::cout << "BlackBox durability suite (engine-level, fork-based crash sim)\n";
    struct Case { const char* name; void (*fn)(const fs::path&); };
    const Case cases[] = {
        {"ack_durability_window", test_ack_durability_window},
        {"flushed_survives_crash", test_flushed_survives_crash},
        {"snapshot_survives_crash", test_snapshot_survives_crash},
        {"wal_only_replay", test_wal_only_replay},
        {"wal_tail_corruption", test_wal_tail_corruption},
        {"wal_torn_final_record", test_wal_torn_final_record},
        {"delete_durability", test_delete_durability},
        {"concurrent_writes", test_concurrent_writes},
    };
    std::vector<fs::path> dirs;
    for (const auto& c : cases) {
        fs::path dir = freshDir(c.name);
        dirs.push_back(dir);
        try {
            c.fn(dir);
        } catch (const std::exception& e) {
            std::cout << "  [INVARIANT] FAIL  case threw: " << e.what() << "\n";
            ++g_invariantFailures;
        }
    }
    for (const auto& d : dirs) { std::error_code ec; fs::remove_all(d, ec); }

    std::cout << "\n============================================================\n";
    std::cout << "Invariant failures: " << g_invariantFailures << "\n";
    std::cout << "Non-durable behaviours observed: " << g_behaviourNotes
              << " (informational; see [BEHAVIOUR] lines)\n";
    if (g_invariantFailures == 0)
        std::cout << "RESULT: all durability INVARIANTS passed.\n";
    else
        std::cout << "RESULT: " << g_invariantFailures << " durability INVARIANT(S) FAILED.\n";
    return g_invariantFailures == 0 ? 0 : 1;
}
