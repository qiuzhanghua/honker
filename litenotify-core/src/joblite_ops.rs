//! Rust implementations of the `jl_*` SQL scalar functions, plus a
//! single `attach_joblite_functions` helper that registers them on a
//! [`rusqlite::Connection`].
//!
//! Consumers:
//!   * `litenotify-extension` — the loadable SQLite extension. Calls
//!     `attach_joblite_functions` so `.load ./liblitenotify_ext` in any
//!     SQLite client exposes the full function set.
//!   * `packages/litenotify` — the PyO3 binding. Calls
//!     `attach_joblite_functions` on its writer connection so Python
//!     can invoke `SELECT jl_*(...)` inside its own transactions
//!     without loading the `.dylib` at runtime.
//!   * Future bindings (Go, Ruby, napi-rs) — load the extension via
//!     SQLite's `sqlite3_load_extension` and get the same functions
//!     for free.
//!
//! Rationale: each per-language binding would otherwise re-implement
//! this SQL. Moving it here gives us one source of truth that's
//! tested once and inherited by every consumer.

use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;

/// Wrap a Displayable error for SQLite scalar-function returns.
fn to_sql_err<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    )))
}

/// Register all `jl_*` joblite scalar functions on `conn`. Idempotent
/// per-connection: creating the same function twice is a rusqlite
/// error, so call exactly once per connection.
pub fn attach_joblite_functions(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function(
        "jl_bootstrap",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            super::bootstrap_joblite_schema(&db).map_err(to_sql_err)?;
            Ok(1i64)
        },
    )?;

    conn.create_scalar_function(
        "jl_claim_batch",
        4,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let n: i64 = ctx.get(2)?;
            let timeout_s: i64 = ctx.get(3)?;
            let db = unsafe { ctx.get_connection() }?;
            claim_batch(&db, &queue, &worker_id, n, timeout_s).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_ack_batch",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let ids_json: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            ack_batch(&db, &ids_json, &worker_id).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_sweep_expired",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            sweep_expired(&db, &queue).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_lock_acquire",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let ttl: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_acquire(&db, &name, &owner, ttl).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_lock_release",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_release(&db, &name, &owner).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_rate_limit_try",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let limit: i64 = ctx.get(1)?;
            let per: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_try(&db, &name, limit, per).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_rate_limit_sweep",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let older_than_s: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_sweep(&db, older_than_s).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_scheduler_record_fire",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let fire_at: i64 = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_record_fire(&db, &name, fire_at).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_scheduler_last_fire",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_last_fire(&db, &name).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_result_save",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let job_id: i64 = ctx.get(0)?;
            let value: String = ctx.get(1)?;
            let ttl_s: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            result_save(&db, job_id, &value, ttl_s).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_result_get",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let job_id: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            result_get(&db, job_id).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "jl_result_sweep",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            result_sweep(&db).map_err(to_sql_err)
        },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------
// Claim / ack
// ---------------------------------------------------------------------

/// Returns JSON text: `[{"id":1,"queue":"...","payload":"...","worker_id":"...","attempts":N,"claim_expires_at":T}, ...]`
pub fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare_cached(
        "UPDATE _joblite_live
         SET state = 'processing',
             worker_id = ?1,
             claim_expires_at = unixepoch() + ?4,
             attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM _joblite_live
           WHERE queue = ?2
             AND state IN ('pending', 'processing')
             AND (expires_at IS NULL OR expires_at > unixepoch())
             AND ((state = 'pending' AND run_at <= unixepoch())
               OR (state = 'processing' AND claim_expires_at < unixepoch()))
           ORDER BY priority DESC, run_at ASC, id ASC
           LIMIT ?3
         )
         RETURNING id, queue, payload, worker_id, attempts, claim_expires_at",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![worker_id, queue, n, timeout_s],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        },
    )?;
    let mut out = String::from("[");
    let mut first = true;
    for row in rows {
        let (id, q, payload, w, attempts, claim_expires_at) = row?;
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&format!(
            "{{\"id\":{},\"queue\":{},\"payload\":{},\"worker_id\":{},\"attempts\":{},\"claim_expires_at\":{}}}",
            id, json_str(&q), json_str(&payload), json_str(&w),
            attempts, claim_expires_at,
        ));
    }
    out.push(']');
    Ok(out)
}

pub fn ack_batch(
    conn: &Connection,
    ids_json: &str,
    worker_id: &str,
) -> rusqlite::Result<i64> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM _joblite_live
         WHERE id IN (SELECT value FROM json_each(?1))
           AND worker_id = ?2
           AND claim_expires_at >= unixepoch()
         RETURNING id",
    )?;
    let mut rows = stmt.query(rusqlite::params![ids_json, worker_id])?;
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

// ---------------------------------------------------------------------
// Task expiration
// ---------------------------------------------------------------------

/// Move expired-pending rows from `_joblite_live` to `_joblite_dead`
/// with `last_error='expired'`. Returns count moved.
pub fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    let mut select = conn.prepare_cached(
        "DELETE FROM _joblite_live
         WHERE queue = ?1
           AND state = 'pending'
           AND expires_at IS NOT NULL
           AND expires_at <= unixepoch()
         RETURNING id, queue, payload, priority, run_at, max_attempts,
                   attempts, created_at",
    )?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, String, i64, i64, i64, i64, i64)> = select
        .query_map(rusqlite::params![queue], |r| {
            Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
                r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(
        "INSERT INTO _joblite_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'expired', ?8)",
    )?;
    let count = rows.len() as i64;
    for r in rows {
        insert.execute(rusqlite::params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7
        ])?;
    }
    Ok(count)
}

// ---------------------------------------------------------------------
// Named locks
// ---------------------------------------------------------------------

pub fn lock_acquire(
    conn: &Connection,
    name: &str,
    owner: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "DELETE FROM _joblite_locks
         WHERE name = ?1 AND expires_at <= unixepoch()",
        rusqlite::params![name],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO _joblite_locks (name, owner, expires_at)
         VALUES (?1, ?2, unixepoch() + ?3)",
        rusqlite::params![name, owner, ttl_s],
    )?;
    let current: Option<String> = conn
        .query_row(
            "SELECT owner FROM _joblite_locks WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .ok();
    Ok(if current.as_deref() == Some(owner) { 1 } else { 0 })
}

pub fn lock_release(
    conn: &Connection,
    name: &str,
    owner: &str,
) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_locks WHERE name = ?1 AND owner = ?2",
        rusqlite::params![name, owner],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

pub fn rate_limit_try(
    conn: &Connection,
    name: &str,
    limit: i64,
    per: i64,
) -> rusqlite::Result<i64> {
    if limit <= 0 || per <= 0 {
        return Err(to_sql_err("limit and per must be positive"));
    }
    let window_start: i64 = conn.query_row(
        "SELECT (unixepoch() / ?1) * ?1",
        rusqlite::params![per],
        |r| r.get(0),
    )?;
    let current: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(count), 0) FROM _joblite_rate_limits
             WHERE name = ?1 AND window_start = ?2",
            rusqlite::params![name, window_start],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if current >= limit {
        return Ok(0);
    }
    conn.execute(
        "INSERT INTO _joblite_rate_limits (name, window_start, count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(name, window_start) DO UPDATE SET count = count + 1",
        rusqlite::params![name, window_start],
    )?;
    Ok(1)
}

pub fn rate_limit_sweep(
    conn: &Connection,
    older_than_s: i64,
) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_rate_limits
         WHERE window_start < unixepoch() - ?1",
        rusqlite::params![older_than_s],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Scheduler state
// ---------------------------------------------------------------------

pub fn scheduler_record_fire(
    conn: &Connection,
    name: &str,
    fire_at_unix: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "INSERT INTO _joblite_scheduler_state (name, last_fire_at)
         VALUES (?1, ?2)
         ON CONFLICT(name) DO UPDATE
           SET last_fire_at = excluded.last_fire_at",
        rusqlite::params![name, fire_at_unix],
    )?;
    Ok(0)
}

pub fn scheduler_last_fire(
    conn: &Connection,
    name: &str,
) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT last_fire_at FROM _joblite_scheduler_state WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Task result storage
// ---------------------------------------------------------------------

pub fn result_save(
    conn: &Connection,
    job_id: i64,
    value: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    if ttl_s > 0 {
        conn.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at)
             VALUES (?1, ?2, unixepoch() + ?3)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = excluded.expires_at",
            rusqlite::params![job_id, value, ttl_s],
        )?;
    } else {
        conn.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = NULL",
            rusqlite::params![job_id, value],
        )?;
    }
    Ok(1)
}

pub fn result_get(
    conn: &Connection,
    job_id: i64,
) -> rusqlite::Result<Option<String>> {
    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT value, expires_at FROM _joblite_results WHERE job_id = ?1",
            rusqlite::params![job_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();
    match row {
        None => Ok(None),
        Some((_, Some(exp))) if exp <= now_unix(conn)? => Ok(None),
        Some((value, _)) => Ok(value),
    }
}

pub fn result_sweep(conn: &Connection) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_results
         WHERE expires_at IS NOT NULL AND expires_at <= unixepoch()",
        [],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn now_unix(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
}

/// Escape a string for inclusion as a JSON string literal. Used by
/// `claim_batch` to build its JSON array return value without
/// pulling in serde_json just for one site.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
