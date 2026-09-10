//! Range and prefix iterators never report a block cache hit.
//!
//! ```text
//! cargo test --features metrics --test iter_cache_metrics -- --nocapture
//! ```
//!
//! `iter_hits_are_never_counted` is expected to FAIL. The control,
//! `point_read_hits_are_counted`, passes: the same cached block, reached by a
//! point read instead of a scan, is reported correctly.
//!
//! `Keyspace::{range, prefix, iter}` build a `table::iter::Iter`, which looks
//! the block up in the cache itself and, on a hit, returns without calling
//! `table::util::load_block`:
//!
//! ```ignore
//! // lsm-tree/src/table/iter.rs:249 and :370
//! let block = match self.cache.get_block(self.table_id, handle.offset()) {
//!     Some(block) => block,      // <- nothing counted
//!     None => load_block(...),   // <- counts the load and the I/O
//! };
//! ```
//!
//! `load_block` already performs that same lookup and increments
//! `data_block_load_cached` when it succeeds (lsm-tree/src/table/util.rs:47),
//! so the pre-check is a duplicate that only skips the instrumentation.
//!
//! The effect is that `block_cache_hit_rate` and its per-kind variants read
//! near zero for any scan-heavy workload, whatever the cache is really doing.
//! A store small enough to be served entirely from cache reports the same
//! numbers as one that never hits at all, so the counters cannot be used to
//! size `cache_size`.

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

const ITEMS: u64 = 20_000;
const VALUE_LEN: usize = 128;

/// Enough rows to fill many data blocks, flushed into segments so that
/// reading them has to go through the block cache at all.
fn filled() -> (tempfile::TempDir, Database, fjall::Keyspace) {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::builder(dir.path()).worker_threads(2).open().unwrap();
    let ks = db
        .keyspace("items", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_024 * 1_024)
        })
        .unwrap();

    let value = vec![b'x'; VALUE_LEN];
    for i in 0..ITEMS {
        ks.insert(i.to_be_bytes(), &value).unwrap();
    }
    db.persist(PersistMode::SyncAll).unwrap();
    ks.rotate_memtable_and_wait().unwrap();

    (dir, db, ks)
}

fn report(label: &str, m: &lsm_tree::Metrics) {
    println!(
        "{label:<28} loads {:>7}  io {:>7}  cached {:>7}  hit rate {:>6.1}%",
        m.block_loads(),
        m.block_load_io_count(),
        m.block_load_cached_count(),
        100.0 * m.block_cache_hit_rate(),
    );
}

#[test]
fn iter_hits_are_never_counted() {
    let (_dir, _db, ks) = filled();

    // First pass: cold, every block read from the filesystem.
    let n = ks.iter().count();
    assert_eq!(n as u64, ITEMS);
    let cold_loads = ks.metrics().block_loads();
    let cold_cached = ks.metrics().block_load_cached_count();
    report("first scan (cold)", ks.metrics());
    assert!(cold_loads > 0, "the scan read no blocks at all");

    // Second pass over the same data. The blocks are in the cache now, so
    // every load should be reported as a hit.
    let n = ks.iter().count();
    assert_eq!(n as u64, ITEMS);
    report("second scan (warm)", ks.metrics());

    let cached = ks.metrics().block_load_cached_count() - cold_cached;
    assert!(
        cached > 0,
        "the second scan reported {cached} cache hits over {} blocks; \
         iterators return cached blocks without counting them",
        cold_loads,
    );
}

/// The control. The same blocks, reached by a point read, are counted.
#[test]
fn point_read_hits_are_counted() {
    let (_dir, _db, ks) = filled();

    // Warm the cache for one key.
    assert!(ks.get(0u64.to_be_bytes()).unwrap().is_some());
    let before = ks.metrics().block_load_cached_count();

    // Read it again: served from the cache, and reported.
    assert!(ks.get(0u64.to_be_bytes()).unwrap().is_some());
    report("repeated point read", ks.metrics());

    let cached = ks.metrics().block_load_cached_count() - before;
    assert!(cached > 0, "a repeated point read reported {cached} cache hits");
}
