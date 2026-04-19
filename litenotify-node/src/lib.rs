//! Node binding for litenotify.
//!
//! Mirrors the Python API, with types tuned for JavaScript:
//!
//!     const lit = require('@litenotify/node');
//!     const db = lit.open('app.db');
//!     const tx = db.transaction();
//!     tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
//!     tx.notify('orders', JSON.stringify({id: 42}));
//!     tx.commit();
//!
//!     const ev = db.walEvents();
//!     while (running) {
//!       await ev.next();
//!       const rows = db.query(
//!         'SELECT * FROM _litenotify_notifications WHERE id > ?',
//!         [lastSeen]);
//!       // ...
//!     }
//!
//! Rows and parameter values are serialized via JSON at the boundary —
//! keeps the binding small and avoids nominal-value-type matching between
//! JS and SQLite. Users can pass numbers, strings, booleans, null,
//! arrays, and objects; objects/arrays get JSON-stringified.
//!
//! NOTE: this crate duplicates some Rust from the PyO3 crate
//! (`litenotify/src/lib.rs`). Both copies work against the same
//! `.db` + `.db-wal` files — interop across processes goes through the
//! filesystem, not shared in-process state. Planned consolidation into
//! a shared `litenotify-core` rlib is out of scope for this commit.

use napi::Result;
use napi_derive::napi;
use parking_lot::{Condvar, Mutex};
use rusqlite::functions::FunctionFlags;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Map, Value as JsonValue};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn napi_err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, e.to_string())
}

// ---------- Shared open + notify attach ----------

fn attach_notify(conn: &Connection) -> rusqlite::Result<()> {
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

fn open_conn(path: &str, install_notify: bool) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size = -32000;
         PRAGMA temp_store = MEMORY;
         PRAGMA wal_autocheckpoint = 10000;",
    )?;
    if install_notify {
        attach_notify(&conn)?;
    }
    Ok(conn)
}

// ---------- JSON <-> SQL param conversion ----------

fn json_to_sql(v: &JsonValue) -> SqlValue {
    match v {
        JsonValue::Null => SqlValue::Null,
        JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Text(n.to_string())
            }
        }
        JsonValue::String(s) => SqlValue::Text(s.clone()),
        // Objects/arrays are SQL-serialized as JSON text — consistent with
        // how joblite.Queue.enqueue(payload) treats dicts/lists.
        JsonValue::Array(_) | JsonValue::Object(_) => SqlValue::Text(v.to_string()),
    }
}

fn row_to_json(columns: &[String], row: &rusqlite::Row) -> rusqlite::Result<JsonValue> {
    let mut obj = Map::new();
    for (i, name) in columns.iter().enumerate() {
        let vref = row.get_ref(i)?;
        let v = match vref {
            ValueRef::Null => JsonValue::Null,
            ValueRef::Integer(n) => JsonValue::from(n),
            ValueRef::Real(f) => JsonValue::from(f),
            ValueRef::Text(t) => JsonValue::from(std::str::from_utf8(t).unwrap_or("")),
            ValueRef::Blob(b) => {
                // Base64 would be more compact, but sticking with hex keeps
                // one fewer dep. Blob return values are uncommon on this
                // binding anyway (most rows are payload JSON strings).
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                JsonValue::from(hex)
            }
        };
        obj.insert(name.clone(), v);
    }
    Ok(JsonValue::Object(obj))
}

fn run_query(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<JsonValue> {
    let mut stmt = conn.prepare_cached(sql).map_err(napi_err)?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(napi_err)?;
    let mut arr: Vec<JsonValue> = Vec::new();
    while let Some(row) = rows.next().map_err(napi_err)? {
        arr.push(row_to_json(&columns, row).map_err(napi_err)?);
    }
    Ok(JsonValue::Array(arr))
}

fn run_execute(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<u32> {
    let mut stmt = conn.prepare_cached(sql).map_err(napi_err)?;
    let n = stmt
        .execute(rusqlite::params_from_iter(params.iter()))
        .map_err(napi_err)?;
    Ok(n as u32)
}

fn sql_params_from_json(arr: Option<Vec<JsonValue>>) -> Vec<SqlValue> {
    arr.unwrap_or_default().iter().map(json_to_sql).collect()
}

fn stat_pair(path: &std::path::Path) -> (u64, i128) {
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

// ---------- Writer / Readers pools ----------

struct Writer {
    slot: Mutex<Option<Connection>>,
    available: Condvar,
}

impl Writer {
    fn new(conn: Connection) -> Self {
        Self {
            slot: Mutex::new(Some(conn)),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) -> Connection {
        let mut guard = self.slot.lock();
        while guard.is_none() {
            self.available.wait(&mut guard);
        }
        guard.take().unwrap()
    }

    fn release(&self, conn: Connection) {
        let mut guard = self.slot.lock();
        *guard = Some(conn);
        self.available.notify_one();
    }
}

struct Readers {
    pool: Mutex<Vec<Connection>>,
    outstanding: Mutex<usize>,
    available: Condvar,
    path: String,
    max: usize,
}

impl Readers {
    fn new(path: String, max: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            outstanding: Mutex::new(0),
            available: Condvar::new(),
            path,
            max: max.max(1),
        }
    }

    fn acquire(&self) -> rusqlite::Result<Connection> {
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

    fn release(&self, conn: Connection) {
        let mut pool = self.pool.lock();
        pool.push(conn);
        self.available.notify_one();
    }
}

// ---------- napi-rs classes ----------

#[napi]
pub struct Database {
    writer: Arc<Writer>,
    readers: Arc<Readers>,
    path: String,
}

#[napi]
impl Database {
    /// Begin a write transaction. Must `.commit()` or `.rollback()`.
    /// Dropping without either rolls back.
    #[napi]
    pub fn transaction(&self) -> Result<Transaction> {
        let conn = self.writer.acquire();
        match conn.execute_batch("BEGIN IMMEDIATE") {
            Ok(()) => Ok(Transaction {
                inner: Arc::new(Mutex::new(TxState {
                    conn: Some(conn),
                    writer: self.writer.clone(),
                    started: true,
                    finished: false,
                })),
            }),
            Err(e) => {
                self.writer.release(conn);
                Err(napi_err(e))
            }
        }
    }

    /// SELECT returns `Array<Object>` (each row is a plain object).
    #[napi(ts_return_type = "Array<Record<string, any>>")]
    pub fn query(
        &self,
        sql: String,
        params: Option<Vec<JsonValue>>,
    ) -> Result<JsonValue> {
        let params = sql_params_from_json(params);
        let conn = self.readers.acquire().map_err(napi_err)?;
        let result = run_query(&conn, &sql, &params);
        self.readers.release(conn);
        result
    }

    /// Filesystem watcher on the .db-wal file. `await ev.next()` resolves
    /// on every commit to the database (any process, any writer).
    #[napi]
    pub fn wal_events(&self) -> Result<WalEvents> {
        let wal_path: PathBuf = format!("{}-wal", self.path).into();
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1024);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        let wal_t = wal_path.clone();
        std::thread::Builder::new()
            .name("litenotify-wal-poll".into())
            .spawn(move || {
                let mut last = stat_pair(&wal_t);
                while !stop_t.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                    let cur = stat_pair(&wal_t);
                    if cur != last {
                        last = cur;
                        let _ = tx.try_send(());
                    }
                }
            })
            .map_err(napi_err)?;
        Ok(WalEvents {
            stop,
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    /// Delete notifications older than a duration, or beyond a count.
    /// Returns number of rows removed.
    #[napi]
    pub fn prune_notifications(
        &self,
        older_than_s: Option<i64>,
        max_keep: Option<i64>,
    ) -> Result<u32> {
        let mut conditions: Vec<&str> = Vec::new();
        let mut params: Vec<SqlValue> = Vec::new();
        if let Some(secs) = older_than_s {
            conditions.push("created_at < unixepoch() - ?");
            params.push(SqlValue::Integer(secs));
        }
        if let Some(k) = max_keep {
            conditions.push("id <= (SELECT MAX(id) - ? FROM _litenotify_notifications)");
            params.push(SqlValue::Integer(k));
        }
        if conditions.is_empty() {
            return Ok(0);
        }
        let sql = format!(
            "DELETE FROM _litenotify_notifications WHERE {}",
            conditions.join(" OR ")
        );
        let conn = self.writer.acquire();
        let result = (|| -> rusqlite::Result<u32> {
            conn.execute_batch("BEGIN IMMEDIATE")?;
            let mut stmt = conn.prepare_cached(&sql)?;
            let n = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            conn.execute_batch("COMMIT")?;
            Ok(n as u32)
        })();
        let final_result = match result {
            Ok(n) => Ok(n),
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(napi_err(e))
            }
        };
        self.writer.release(conn);
        final_result
    }
}

struct TxState {
    conn: Option<Connection>,
    writer: Arc<Writer>,
    started: bool,
    finished: bool,
}

impl Drop for TxState {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.started && !self.finished {
                let _ = conn.execute_batch("ROLLBACK");
            }
            self.writer.release(conn);
        }
    }
}

#[napi]
pub struct Transaction {
    inner: Arc<Mutex<TxState>>,
}

#[napi]
impl Transaction {
    #[napi]
    pub fn execute(
        &self,
        sql: String,
        params: Option<Vec<JsonValue>>,
    ) -> Result<u32> {
        let params = sql_params_from_json(params);
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        run_execute(conn, &sql, &params)
    }

    #[napi(ts_return_type = "Array<Record<string, any>>")]
    pub fn query(
        &self,
        sql: String,
        params: Option<Vec<JsonValue>>,
    ) -> Result<JsonValue> {
        let params = sql_params_from_json(params);
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        run_query(conn, &sql, &params)
    }

    /// Publish a cross-process notification. `payload` should be a
    /// JSON string (caller JSON-encodes); this keeps the wire shape
    /// explicit and matches the Python side.
    #[napi]
    pub fn notify(&self, channel: String, payload: String) -> Result<i64> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let id: i64 = conn
            .query_row(
                "SELECT notify(?1, ?2)",
                rusqlite::params![channel, payload],
                |r| r.get(0),
            )
            .map_err(napi_err)?;
        Ok(id)
    }

    #[napi]
    pub fn commit(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.finished {
            return Ok(());
        }
        let conn = state
            .conn
            .take()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let result = match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(napi_err(e))
            }
        };
        state.writer.release(conn);
        state.finished = true;
        state.started = false;
        result
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.finished {
            return Ok(());
        }
        let conn = state
            .conn
            .take()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let _ = conn.execute_batch("ROLLBACK");
        state.writer.release(conn);
        state.finished = true;
        state.started = false;
        Ok(())
    }
}

#[napi]
pub struct WalEvents {
    stop: Arc<AtomicBool>,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[napi]
impl WalEvents {
    /// Await the next WAL change. Resolves on every DB commit.
    #[napi]
    pub async fn next(&self) -> Result<()> {
        let rx = self.rx.clone();
        tokio::task::spawn_blocking(move || {
            let r = rx.lock();
            r.recv().map_err(napi_err)
        })
        .await
        .map_err(napi_err)??;
        Ok(())
    }

    /// Stop the background stat-poll thread.
    #[napi]
    pub fn close(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

// ---------- module entry ----------

#[napi]
pub fn open(path: String, max_readers: Option<u32>) -> Result<Database> {
    let max_readers = max_readers.unwrap_or(8).max(1) as usize;
    let writer_conn = open_conn(&path, true).map_err(napi_err)?;
    Ok(Database {
        writer: Arc::new(Writer::new(writer_conn)),
        readers: Arc::new(Readers::new(path.clone(), max_readers)),
        path,
    })
}
