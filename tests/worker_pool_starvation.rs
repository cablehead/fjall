//! Reproduces a flush-starvation deadlock in the per-database worker pool.
//!
//! ```text
//! cargo test --test worker_pool_starvation -- --nocapture --test-threads=1
//! ```
//!
//! `one_worker_starves` panics with `worker pool starved` while the bug is
//! present (`#[should_panic]`, so `cargo test` is green when the deadlock
//! actually fires). `two_workers_keep_up` is an empirical control: a second
//! worker usually keeps the rotation queue from filling. It is not a proof
//! that two receivers unblock `send(Flush)`.

use fjall::{Database, KeyspaceCreateOptions};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
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

fn write_load(keyspace: &fjall::Keyspace, progress: &Progress) -> fjall::Result<()> {
    let value = vec![b'v'; VALUE_LEN];

    for i in 0..INSERTS {
        keyspace.insert(i.to_be_bytes(), &value)?;
        progress.inserts.store(i + 1, Relaxed);

        if i % 200 == 0 {
            let segments = keyspace.disk_space();

            if segments > progress.segment_bytes.load(Relaxed) {
                progress.segment_bytes.store(segments, Relaxed);
                progress.segments_last_grew_at.store(i, Relaxed);
            }
        }
    }

    Ok(())
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
            let _ = writer_tx.send(write_load(&keyspace, &progress));
        }
    });

    let writer = writer_rx.recv_timeout(WRITER_BUDGET);

    // Abandon this thread; joining it would hang if the pool is starved.
    let (rotate_tx, rotate_rx) = mpsc::channel();

    std::thread::spawn({
        let keyspace = keyspace.clone();
        move || {
            let _ = rotate_tx.send(keyspace.rotate_memtable_and_wait());
        }
    });

    let rotate = rotate_rx.recv_timeout(ROTATE_BUDGET);

    // After the bounded rotate so a healthy pool is not penalized for a flush
    // still in flight when the writer stops.
    let segment_bytes = keyspace.disk_space();

    let inserts = progress.inserts.load(Relaxed);
    let bytes_written = inserts * (8 + VALUE_LEN as u64);
    let last_grew_at = progress.segments_last_grew_at.load(Relaxed);
    let flushed_pct = if bytes_written == 0 {
        0.0
    } else {
        segment_bytes as f64 / bytes_written as f64 * 100.0
    };

    // Directory size is not a flush metric: the journal is preallocated to 64 MiB.
    let journal_bytes = db.journal_disk_space().expect("should read journal size");

    let writer_finished = matches!(writer, Ok(Ok(())));
    let rotate_returned = matches!(rotate, Ok(Ok(())));

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

    let pool_stuck = matches!(writer, Err(RecvTimeoutError::Timeout))
        || matches!(rotate, Err(RecvTimeoutError::Timeout));

    if pool_stuck {
        // Dropping the database spins forever waiting for its worker threads,
        // and the writer may still be running in the folder.
        std::mem::forget(keyspace);
        std::mem::forget(db);
        std::mem::forget(folder);
    }

    match writer {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("writer insert failed: {e:?}"),
        Err(RecvTimeoutError::Timeout) => panic!(
            "writer did not finish within {WRITER_BUDGET:?} \
             (inserts {inserts} of {INSERTS})"
        ),
        Err(RecvTimeoutError::Disconnected) => panic!("writer thread panicked"),
    }

    match rotate {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("rotate_memtable_and_wait failed: {e:?}"),
        Err(RecvTimeoutError::Timeout) => panic!(
            "worker pool starved: rotate_memtable_and_wait blocked for {ROTATE_BUDGET:?} \
             ({flushed_pct:.1}% of written bytes reached segments, \
             segment growth stopped at insert {last_grew_at} of {INSERTS})"
        ),
        Err(RecvTimeoutError::Disconnected) => {
            panic!("rotate_memtable_and_wait thread panicked")
        }
    }

    assert!(
        flushed_pct >= MIN_FLUSHED_PCT,
        "only {flushed_pct:.1}% of the written bytes reached segments \
         ({segment_bytes} of {bytes_written}), segment growth stopped at insert {last_grew_at}",
    );
}

#[test]
#[should_panic(expected = "worker pool starved")]
fn one_worker_starves() {
    run(1);
}

#[test]
fn two_workers_keep_up() {
    run(2);
}
