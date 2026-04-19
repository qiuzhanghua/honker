// Smoke test for the Node binding. Run with `node --test test/`.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const lit = require('..');

function tmpdb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'litenotify-node-'));
  return { path: path.join(dir, 't.db'), cleanup: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

test('open / transaction / commit', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    const db = lit.open(dbPath);
    const tx = db.transaction();
    tx.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)');
    tx.execute('INSERT INTO t (payload) VALUES (?)', ['hello']);
    tx.commit();
    const rows = db.query('SELECT id, payload FROM t ORDER BY id');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].payload, 'hello');
  } finally {
    cleanup();
  }
});

test('rollback drops writes', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    const db = lit.open(dbPath);
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (v INTEGER)');
      tx.commit();
    }
    {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (v) VALUES (1)');
      tx.rollback();
    }
    const rows = db.query('SELECT COUNT(*) AS c FROM t');
    assert.equal(rows[0].c, 0);
  } finally {
    cleanup();
  }
});

test('notify inserts into _litenotify_notifications and rollback drops it', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    const db = lit.open(dbPath);

    // Committed notify
    {
      const tx = db.transaction();
      const id = tx.notify('orders', JSON.stringify({ id: 42 }));
      assert.ok(id > 0);
      tx.commit();
    }
    // Rolled-back notify
    {
      const tx = db.transaction();
      tx.notify('orders', JSON.stringify({ id: 99 }));
      tx.rollback();
    }
    const rows = db.query(
      "SELECT channel, payload FROM _litenotify_notifications WHERE channel='orders' ORDER BY id"
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].channel, 'orders');
    assert.deepEqual(JSON.parse(rows[0].payload), { id: 42 });
  } finally {
    cleanup();
  }
});

test('walEvents fires on commit', async () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    const db = lit.open(dbPath);
    // Force WAL file to exist by doing a first commit.
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }
    const ev = db.walEvents();
    // Fire a commit after a short delay; walEvents.next() should resolve.
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    const t0 = performance.now();
    await ev.next();
    const dt = performance.now() - t0;
    assert.ok(dt < 500, `WAL wake took ${dt.toFixed(1)}ms, expected < 500`);
    ev.close();
  } finally {
    cleanup();
  }
});

test('pruneNotifications by max_keep', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    const db = lit.open(dbPath);
    for (let i = 0; i < 50; i++) {
      const tx = db.transaction();
      tx.notify('ch', `n${i}`);
      tx.commit();
    }
    const before = db.query('SELECT COUNT(*) AS c FROM _litenotify_notifications');
    assert.equal(before[0].c, 50);
    const deleted = db.pruneNotifications(null, 5);
    assert.equal(deleted, 45);
    const after = db.query('SELECT COUNT(*) AS c FROM _litenotify_notifications');
    assert.equal(after[0].c, 5);
  } finally {
    cleanup();
  }
});
