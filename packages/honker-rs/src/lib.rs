//! Ergonomic Rust wrapper over `honker-core` — Durable queues,
//! streams, pub/sub, and scheduler on SQLite.
//!
//! ```no_run
//! use honker::{Database, EnqueueOpts, QueueOpts};
//! use serde_json::json;
//!
//! let db = Database::open("app.db").unwrap();
//! let q = db.queue("emails", QueueOpts::default());
//!
//! q.enqueue(&json!({"to": "alice@example.com"}), EnqueueOpts::default()).unwrap();
//!
//! if let Some(job) = q.claim_one("worker-1").unwrap() {
//!     // ... process ...
//!     job.ack().unwrap();
//! }
//! ```
//!
//! This crate opens its own connection + registers every `honker_*`
//! SQL function via `honker_core::attach_honker_functions`. No
//! `.dylib` load needed at runtime — the functions compile in with
//! the crate. Use the loadable extension path (`honker-extension`)
//! only when mixing with other SQLite clients (e.g. the `sqlite3`
//! CLI or other-language bindings on the same file).

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("core error: {0}")]
    Core(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A Honker database handle. Wraps a single `rusqlite::Connection`
/// configured with WAL mode, the default PRAGMAs, and every
/// `honker_*` SQL function pre-registered.
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Open (or create) a SQLite database at `path`. Applies default
    /// PRAGMAs, registers the honker SQL functions, and bootstraps
    /// the schema.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().into_owned();
        let conn = honker_core::open_conn(&path_str, true)
            .map_err(|e| Error::Core(e.to_string()))?;
        honker_core::attach_honker_functions(&conn)?;
        honker_core::bootstrap_honker_schema(&conn)
            .map_err(|e| Error::Core(e.to_string()))?;
        Ok(Self { conn })
    }

    /// Get a named queue handle. Opts default to 300s visibility,
    /// 3 max attempts.
    pub fn queue(&self, name: &str, opts: QueueOpts) -> Queue<'_> {
        Queue {
            db: self,
            name: name.to_string(),
            opts,
        }
    }

    /// Fire a `pg_notify`-style pub/sub signal. Payload is any
    /// `Serialize` type (unlike `notify()` in raw SQL, which takes
    /// a string — this wrapper JSON-encodes on your behalf).
    /// Returns the notification row id.
    pub fn notify<P: Serialize>(&self, channel: &str, payload: &P) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        let id = self.conn.query_row(
            "SELECT notify(?1, ?2)",
            params![channel, json],
            |r| r.get(0),
        )?;
        Ok(id)
    }

    /// Escape hatch: the underlying connection for advanced queries
    /// (e.g. reading `_honker_live` directly or joining to your own
    /// business tables in the same transaction).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

/// Per-queue configuration.
#[derive(Debug, Clone)]
pub struct QueueOpts {
    pub visibility_timeout_s: i64,
    pub max_attempts: i64,
}

impl Default for QueueOpts {
    fn default() -> Self {
        Self {
            visibility_timeout_s: 300,
            max_attempts: 3,
        }
    }
}

pub struct Queue<'a> {
    db: &'a Database,
    name: String,
    opts: QueueOpts,
}

/// Per-enqueue options. `Delay` / `RunAt` / `Expires` use `Option<i64>`
/// so callers can distinguish unset from zero.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOpts {
    pub delay: Option<i64>,
    pub run_at: Option<i64>,
    pub priority: i64,
    pub expires: Option<i64>,
}

impl<'a> Queue<'a> {
    /// Enqueue a job. Payload is serialized via `serde_json`.
    /// Returns the new row's id.
    pub fn enqueue<P: Serialize>(&self, payload: &P, opts: EnqueueOpts) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        let id = self.db.conn.query_row(
            "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                self.name,
                json,
                opts.run_at,
                opts.delay,
                opts.priority,
                self.opts.max_attempts,
                opts.expires
            ],
            |r| r.get(0),
        )?;
        Ok(id)
    }

    /// Atomically claim up to `n` jobs.
    pub fn claim_batch(&self, worker_id: &str, n: i64) -> Result<Vec<Job<'a>>> {
        let rows_json: String = self.db.conn.query_row(
            "SELECT honker_claim_batch(?1, ?2, ?3, ?4)",
            params![self.name, worker_id, n, self.opts.visibility_timeout_s],
            |r| r.get(0),
        )?;
        let raw: Vec<RawJob> = serde_json::from_str(&rows_json)?;
        Ok(raw
            .into_iter()
            .map(|r| Job {
                db: self.db,
                id: r.id,
                queue: r.queue,
                payload: r.payload.into_bytes(),
                worker_id: r.worker_id,
                attempts: r.attempts,
            })
            .collect())
    }

    /// Claim a single job, or `None` if the queue is empty.
    pub fn claim_one(&self, worker_id: &str) -> Result<Option<Job<'a>>> {
        Ok(self.claim_batch(worker_id, 1)?.into_iter().next())
    }
}

#[derive(Deserialize)]
struct RawJob {
    id: i64,
    queue: String,
    payload: String,
    worker_id: String,
    attempts: i64,
    #[serde(rename = "claim_expires_at")]
    #[allow(dead_code)]
    claim_expires_at: i64,
}

/// A claimed unit of work. `payload` is the raw JSON bytes — deserialize
/// into your own struct via `serde_json::from_slice(&job.payload)`.
pub struct Job<'a> {
    db: &'a Database,
    pub id: i64,
    pub queue: String,
    pub payload: Vec<u8>,
    pub worker_id: String,
    pub attempts: i64,
}

impl<'a> Job<'a> {
    /// Deserialize the payload into `T`. Convenience for
    /// `serde_json::from_slice(&job.payload)`.
    pub fn payload_as<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }

    /// DELETE the row if the claim is still valid. Returns true iff
    /// the claim hadn't expired.
    pub fn ack(&self) -> Result<bool> {
        let n: i64 = self.db.conn.query_row(
            "SELECT honker_ack(?1, ?2)",
            params![self.id, self.worker_id],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Put the job back with a delay, or move to dead after max_attempts.
    pub fn retry(&self, delay_s: i64, error: &str) -> Result<bool> {
        let n: i64 = self.db.conn.query_row(
            "SELECT honker_retry(?1, ?2, ?3, ?4)",
            params![self.id, self.worker_id, delay_s, error],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Unconditionally move the claim to `_honker_dead`.
    pub fn fail(&self, error: &str) -> Result<bool> {
        let n: i64 = self.db.conn.query_row(
            "SELECT honker_fail(?1, ?2, ?3)",
            params![self.id, self.worker_id, error],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Extend the visibility timeout.
    pub fn heartbeat(&self, extend_s: i64) -> Result<bool> {
        let n: i64 = self.db.conn.query_row(
            "SELECT honker_heartbeat(?1, ?2, ?3)",
            params![self.id, self.worker_id, extend_s],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }
}
