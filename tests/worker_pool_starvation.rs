//! Reproduces a flush-starvation deadlock in the per-database worker pool.
//!
//! ```text
//! cargo test --test worker_pool_starvation -- --nocapture --test-threads=1
//! ```
//!
//! `one_worker_starves` is expected to FAIL and `two_workers_keep_up` to pass.
//! The two tests are identical apart from the `worker_threads` argument. Each
//! takes about three seconds.
//!
//! The chain, all inside this crate:
//!
//! 1. Every write calls `Keyspace::check_memtable_rotate`. Once the active
//!    memtable is over `max_memtable_size`, each write calls `request_rotation`,
//!    which `try_send`s a `WorkerMessage::RotateMemtable` into the pool's
//!    `flume::bounded(1000)` channel and silently drops it when the channel is
//!    full.
//! 2. While the worker is busy flushing or compacting nothing receives, so the
//!    writer fills all 1000 slots with rotation requests for the memtable that
//!    has not been rotated yet.
//! 3. The worker comes back, pops one, and `Keyspace::inner_rotate_memtable`
//!    does a *blocking* `send(WorkerMessage::Flush)` on that same channel.
//!
//! With one worker that thread is the only receiver, so the blocking send waits
//! on itself. Flushes and compactions stop permanently: writes keep succeeding
//! into an ever growing memtable, but no new segment is ever written.
//!
//! `rotate_memtable_and_wait` never returns either, and not because it is
//! waiting for a flush: it reaches the same blocking send in
//! `inner_rotate_memtable` and joins the pile-up.
//!
//! One keyspace, one writer thread and plain `insert` are enough. Neither write
//! batches nor several keyspaces are needed, they only make the channel fill
//! faster. The small `max_memtable_size` below only makes the test quick; the
//! default 64 MiB deadlocks the same way, it just takes around 400k inserts of
//! 1 KiB values to get there.
//!
//! Nothing here waits on a flush from the test thread. A starved pool never
//! makes progress again, so every wait is bounded and reported rather than hung.

use fjall::{Database, KeyspaceCreateOptions};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use test_log::test;

const MAX_MEMTABLE_SIZE: u64 = 128 * 1_024;
const INSERTS: u64 = 100_000;
const VALUE_LEN: usize = 64;

/// Share of the written bytes that must reach segments. A healthy pool lands at
/// ~100%, a starved one at 1-4%.
const MIN_FLUSHED_PCT: f64 = 50.0;

const WRITER_BUDGET: Duration = Duration::from_secs(60);
const ROTATE_BUDGET: Duration = Duration::from_secs(5);

/// Written by the writer thread so the test thread can report progress even if
/// the writer never finishes.
#[derive(Default)]
struct Progress {
    inserts: AtomicU64,
    segment_bytes: AtomicU64,
    segments_last_grew_at: AtomicU64,
}

fn run(worker_threads: usize) {
    let folder = tempfile::tempdir().expect("should create temp dir");

    let db = Database::builder(&folder)
        .worker_threads(worker_threads)
        .open()
        .expect("should open database");

    let keyspace = db
        .keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(MAX_MEMTABLE_SIZE)
        })
        .expect("should create keyspace");

    let progress = Arc::new(Progress::default());
    let (writer_tx, writer_rx) = mpsc::channel();

    std::thread::spawn({
        let keyspace = keyspace.clone();
        let progress = progress.clone();

        move || {
            let value = vec![b'v'; VALUE_LEN];

            for i in 0..INSERTS {
                keyspace.insert(i.to_be_bytes(), &value).expect("insert");
                progress.inserts.store(i + 1, Relaxed);

                // Sample how much of the data has reached segments, and remember
                // the insert at which that stopped moving.
                if i % 200 == 0 {
                    let segments = keyspace.disk_space();

                    if segments > progress.segment_bytes.load(Relaxed) {
                        progress.segment_bytes.store(segments, Relaxed);
                        progress.segments_last_grew_at.store(i, Relaxed);
                    }
                }
            }

            let _ = writer_tx.send(());
        }
    });

    let writer_finished = writer_rx.recv_timeout(WRITER_BUDGET).is_ok();

    // `rotate_memtable_and_wait` cannot return once the pool is starved: it runs
    // straight into the same blocking send. Abandon the thread, never join it.
    let (rotate_tx, rotate_rx) = mpsc::channel();

    std::thread::spawn({
        let keyspace = keyspace.clone();
        move || {
            let _ = rotate_tx.send(keyspace.rotate_memtable_and_wait());
        }
    });

    let rotate_returned = rotate_rx.recv_timeout(ROTATE_BUDGET).is_ok();

    // Measured after the bounded rotate, so a healthy pool is not penalized for
    // the flush that is still in flight when the writer stops.
    let segment_bytes = keyspace.disk_space();

    let inserts = progress.inserts.load(Relaxed);
    let bytes_written = inserts * (8 + VALUE_LEN as u64);
    let last_grew_at = progress.segments_last_grew_at.load(Relaxed);
    let flushed_pct = segment_bytes as f64 / bytes_written as f64 * 100.0;

    // NOT part of the metric: the journal is preallocated to 64 MiB, so
    // directory size says nothing about what actually got flushed.
    let journal_bytes = db.journal_disk_space().expect("should read journal size");

    println!("worker_threads({worker_threads})");
    println!("  inserts        {inserts} of {INSERTS} (writer finished: {writer_finished})");
    println!("  bytes written  {bytes_written}");
    println!("  segments       {segment_bytes} ({flushed_pct:.1}% of bytes written)");
    println!("  journal        {journal_bytes} (preallocated, not a flush metric)");
    println!(
        "  segment growth stopped at insert {last_grew_at} ({:.1}% into the run)",
        last_grew_at as f64 / INSERTS as f64 * 100.0,
    );
    println!("  rotate_memtable_and_wait returned within {ROTATE_BUDGET:?}: {rotate_returned}");

    if !writer_finished || !rotate_returned {
        // The pool is stuck. Dropping the database spins forever waiting for its
        // worker threads, and the writer may still be running in the folder.
        std::mem::forget(keyspace);
        std::mem::forget(db);
        std::mem::forget(folder);
    }

    assert!(
        writer_finished,
        "writer did not finish within {WRITER_BUDGET:?}",
    );
    assert!(
        flushed_pct >= MIN_FLUSHED_PCT,
        "only {flushed_pct:.1}% of the written bytes reached segments \
         ({segment_bytes} of {bytes_written}), segment growth stopped at insert {last_grew_at}",
    );
    assert!(
        rotate_returned,
        "rotate_memtable_and_wait did not return within {ROTATE_BUDGET:?}",
    );
}

#[test]
fn one_worker_starves() {
    run(1);
}

#[test]
fn two_workers_keep_up() {
    run(2);
}
