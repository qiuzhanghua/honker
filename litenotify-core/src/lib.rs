//! Shared Rust core for the litenotify bindings.
//!
//! This crate is NOT intended for direct use. It's the plain-Rust
//! foundation that three binding crates depend on:
//!
//!   * `litenotify`           — PyO3 Python extension
//!   * `litenotify-extension` — SQLite loadable extension (cdylib)
//!   * `litenotify-node`      — napi-rs Node.js binding
//!
//! Moving this code here once avoids the three-copies-of-the-same-SQL
//! problem every binding would otherwise suffer. Behavioral drift
//! between the three bindings was a real risk — one would get a new
//! PRAGMA, one wouldn't, and silent inconsistencies would surface only
//! when a Python process and a Node process tried to share a `.db`
//! file.
//!
//! What's here:
//!
//!   - [`open_conn`] — open a SQLite connection with the library's
//!     PRAGMA defaults (WAL, synchronous=NORMAL, 32MB cache, etc.).
//!   - [`attach_notify`] — create `_litenotify_notifications` and
//!     register the `notify(channel, payload)` SQL scalar function.
//!   - [`Writer`] — single-connection write slot with blocking
//!     acquire, non-blocking try_acquire, and release.
//!   - [`Readers`] — bounded pool of reader connections that open
//!     lazily up to a max.
//!   - [`stat_pair`] / [`WalWatcher`] — 1 ms stat-polling thread that
//!     fires a callback on every `.db-wal` change. Bindings wrap this
//!     to surface wake events to their language's async primitive.
//!
//! Anything language-specific — PyO3 classes, napi classes, SQLite
//! entry-point symbols, row-materialization into Python dicts or JS
//! objects — stays in the respective binding crate.

use parking_lot::{Condvar, Mutex};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

// ---------------------------------------------------------------------
// PRAGMAs
// ---------------------------------------------------------------------

/// Default PRAGMA block applied on every connection open. Rationale:
///
///   * `journal_mode=WAL`        — concurrent readers with one writer.
///   * `synchronous=NORMAL`      — fsync WAL at checkpoint, not every
///     commit. Safe against app crashes; OS crashes may lose the last
///     few unchecked-pointed transactions.
///   * `busy_timeout=5000`       — wait up to 5s for the writer lock
///     before returning SQLITE_BUSY.
///   * `foreign_keys=ON`         — enforce FK constraints (off by
///     default in SQLite, a real footgun).
///   * `cache_size=-32000`       — 32MB page cache (default was 2MB).
///   * `temp_store=MEMORY`       — temp B-trees in RAM, not disk.
///   * `wal_autocheckpoint=10000`— fsync every 10k WAL pages. Reduces
///     fsync frequency 10× vs the default of 1k.
pub const DEFAULT_PRAGMAS: &str = "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size = -32000;
         PRAGMA temp_store = MEMORY;
         PRAGMA wal_autocheckpoint = 10000;";

/// Apply the library's default PRAGMAs to an already-open connection.
/// Idempotent.
pub fn apply_default_pragmas(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(DEFAULT_PRAGMAS)
}

// ---------------------------------------------------------------------
// notify() SQL function + notifications schema
// ---------------------------------------------------------------------

/// Install the `_litenotify_notifications` table and the
/// `notify(channel, payload)` SQL scalar function on `conn`. Idempotent.
///
/// `notify()` is the public cross-process primitive. Callers do:
///
/// ```sql
/// BEGIN IMMEDIATE;
/// INSERT INTO orders ...;
/// SELECT notify('orders', '{"id":42}');
/// COMMIT;
/// ```
///
/// The scalar function returns the INSERTed row id. Listeners watch
/// the `.db-wal` file for change and SELECT new rows by channel.
///
/// Pruning is NOT done here. Callers invoke
/// `Database.prune_notifications(older_than_s, max_keep)` when they want
/// to trim the table. No magic timer.
pub fn attach_notify(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _litenotify_notifications (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           channel TEXT NOT NULL,
           payload TEXT NOT NULL,
           created_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX IF NOT EXISTS _litenotify_notifications_recent
           ON _litenotify_notifications(channel, id);",
    )?;

    conn.create_scalar_function(
        "notify",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let channel: String = ctx.get(0)?;
            let payload: String = ctx.get(1)?;
            // Inside a user tx; INSERT inherits the tx. Rollback drops
            // the notification atomically with whatever else rolled back.
            let db = unsafe { ctx.get_connection() }?;
            let mut ins = db.prepare_cached(
                "INSERT INTO _litenotify_notifications (channel, payload) VALUES (?1, ?2)",
            )?;
            let id = ins.insert(rusqlite::params![channel, payload])?;
            Ok(id)
        },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------
// Opening connections
// ---------------------------------------------------------------------

/// Open a SQLite connection at `path` with the library's PRAGMA
/// defaults. If `install_notify` is true, also attach the notifications
/// table + `notify()` SQL function. Readers don't need it; only the
/// writer connection does.
pub fn open_conn(path: &str, install_notify: bool) -> Result<Connection, Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    apply_default_pragmas(&conn)?;
    if install_notify {
        attach_notify(&conn)?;
    }
    Ok(conn)
}

// ---------------------------------------------------------------------
// Writer slot
// ---------------------------------------------------------------------

/// Single-connection write slot. Writers serialize through one
/// rusqlite `Connection` because WAL mode allows only one writer at a
/// time anyway; doing it in user space avoids busy-timeout retries.
pub struct Writer {
    slot: Mutex<Option<Connection>>,
    available: Condvar,
}

impl Writer {
    pub fn new(conn: Connection) -> Self {
        Self {
            slot: Mutex::new(Some(conn)),
            available: Condvar::new(),
        }
    }

    /// Blocking acquire. Waits on a condvar if the slot is held.
    pub fn acquire(&self) -> Connection {
        let mut guard = self.slot.lock();
        while guard.is_none() {
            self.available.wait(&mut guard);
        }
        guard.take().unwrap()
    }

    /// Non-blocking. Returns `Some(conn)` if the slot was immediately
    /// free, else `None`. Bindings use this for a fast path that
    /// avoids GIL release (Python) or async thread-hops (Node) when
    /// the slot is uncontended.
    pub fn try_acquire(&self) -> Option<Connection> {
        let mut guard = self.slot.lock();
        guard.take()
    }

    pub fn release(&self, conn: Connection) {
        let mut guard = self.slot.lock();
        *guard = Some(conn);
        self.available.notify_one();
    }
}

// ---------------------------------------------------------------------
// Reader pool
// ---------------------------------------------------------------------

/// Bounded pool of reader connections. Readers are cheap (one file
/// descriptor + a page cache) and WAL mode allows any number to run
/// concurrently with the writer.
pub struct Readers {
    pool: Mutex<Vec<Connection>>,
    outstanding: Mutex<usize>,
    available: Condvar,
    path: String,
    max: usize,
}

impl Readers {
    pub fn new(path: String, max: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            outstanding: Mutex::new(0),
            available: Condvar::new(),
            path,
            max: max.max(1),
        }
    }

    /// Acquire a reader. Pops a pooled one if available; otherwise
    /// opens a new connection up to `max`. Above `max`, waits on the
    /// condvar.
    pub fn acquire(&self) -> Result<Connection, Error> {
        loop {
            let mut pool = self.pool.lock();
            if let Some(c) = pool.pop() {
                return Ok(c);
            }
            let mut out = self.outstanding.lock();
            if *out < self.max {
                *out += 1;
                drop(out);
                drop(pool);
                return open_conn(&self.path, false);
            }
            drop(out);
            self.available.wait(&mut pool);
        }
    }

    pub fn release(&self, conn: Connection) {
        let mut pool = self.pool.lock();
        pool.push(conn);
        self.available.notify_one();
    }
}

// ---------------------------------------------------------------------
// WAL file watcher
// ---------------------------------------------------------------------

/// Snapshot of the WAL file's `(size, mtime_ns)`. Both 0 if the file
/// does not exist. Compared as a tuple across polls to detect change.
/// The WAL grows on every commit in WAL mode and is truncated on
/// checkpoint; either direction produces a visible delta.
pub fn stat_pair(path: &Path) -> (u64, i128) {
    match std::fs::metadata(path) {
        Ok(m) => {
            let len = m.len();
            let mt = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i128)
                .unwrap_or(0);
            (len, mt)
        }
        Err(_) => (0, 0),
    }
}

/// Background thread that stat()s a `.db-wal` file every 1 ms and
/// fires a callback on every observed change.
///
/// Bindings construct this and wire the callback to whatever async
/// primitive their language uses (Python `asyncio.Queue`, Node
/// mpsc channel drained into a Promise, etc). The watcher thread
/// itself is language-agnostic.
///
/// Why stat-polling over `inotify` / `kqueue` / `FSEvents`? macOS
/// FSEvents (the default on darwin via the `notify` crate) silently
/// drops same-process writes, which means a listener and an enqueuer
/// in the same Python process would never see each other. A dedicated
/// 1 ms stat loop works identically on every platform at ~0 CPU cost.
pub struct WalWatcher {
    stop: Arc<AtomicBool>,
}

impl WalWatcher {
    /// Spawn a watcher thread on `wal_path`. `on_change` is called
    /// once per observed change. The thread runs until [`WalWatcher`]
    /// is dropped or [`stop`](Self::stop) is called.
    pub fn spawn<F>(wal_path: PathBuf, on_change: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        std::thread::Builder::new()
            .name("litenotify-wal-poll".into())
            .spawn(move || {
                let mut last = stat_pair(&wal_path);
                while !stop_t.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                    let cur = stat_pair(&wal_path);
                    if cur != last {
                        last = cur;
                        on_change();
                    }
                }
            })
            .expect("spawn wal-poll thread");
        Self { stop }
    }

    /// Request the watcher thread to stop. Idempotent. Dropping the
    /// `WalWatcher` also stops the thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl Drop for WalWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn mem() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn notify_inserts_row() {
        let conn = mem();
        attach_notify(&conn).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('orders', 'new')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _litenotify_notifications WHERE channel='orders'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn rollback_drops_notification() {
        let conn = mem();
        attach_notify(&conn).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('x', 'y')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("ROLLBACK;").unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _litenotify_notifications",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn writer_try_acquire_returns_none_when_held() {
        let w = Writer::new(Connection::open_in_memory().unwrap());
        let conn = w.acquire();
        assert!(w.try_acquire().is_none());
        w.release(conn);
        assert!(w.try_acquire().is_some());
    }

    #[test]
    fn stat_pair_handles_missing_file() {
        let (size, mt) = stat_pair(std::path::Path::new("/nonexistent/path/nope"));
        assert_eq!(size, 0);
        assert_eq!(mt, 0);
    }
}
