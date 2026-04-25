#!/usr/bin/env python3
"""
proof_data_version.py — Empirical proof that PRAGMA data_version is global.

This script demonstrates why honker uses PRAGMA data_version instead of
SQLITE_FCNTL_DATA_VERSION (file control) for cross-process wake:

  - PRAGMA data_version     → global counter, visible across ALL connections
  - SQLITE_FCNTL_DATA_VERSION → per-pager counter, only changes for the
                                SAME connection's writes

The two share a name but have different semantics. The SQLite docs say
PRAGMA data_version "changes whenever the database file is modified" which
implies global visibility. The file-control opcode returns the pager-local
counter, making it useless for detecting writes from other processes.

Run: python3 scripts/proof_data_version.py
"""

import sqlite3
import tempfile
import os


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")

        conn_a = sqlite3.connect(db)
        conn_b = sqlite3.connect(db)
        conn_a.execute("PRAGMA journal_mode = WAL")
        conn_a.commit()

        print("=" * 60)
        print("Proof: PRAGMA data_version detects cross-connection commits")
        print("=" * 60)
        print(f"Database: {db}")
        print()

        a_before = conn_a.execute("PRAGMA data_version").fetchone()[0]
        b_before = conn_b.execute("PRAGMA data_version").fetchone()[0]
        print(f"Before any writes:")
        print(f"  Connection A data_version: {a_before}")
        print(f"  Connection B data_version: {b_before}")
        print()

        conn_a.execute("CREATE TABLE t(x INTEGER)")
        conn_a.commit()
        a_after_a = conn_a.execute("PRAGMA data_version").fetchone()[0]
        b_after_a = conn_b.execute("PRAGMA data_version").fetchone()[0]
        print(f"After A writes (CREATE TABLE):")
        print(f"  Connection A data_version: {a_after_a}  (+{a_after_a - a_before})")
        print(f"  Connection B data_version: {b_after_a}  (+{b_after_a - b_before})")
        print()

        conn_b.execute("INSERT INTO t VALUES (42)")
        conn_b.commit()
        a_after_b = conn_a.execute("PRAGMA data_version").fetchone()[0]
        b_after_b = conn_b.execute("PRAGMA data_version").fetchone()[0]
        print(f"After B writes (INSERT):")
        print(f"  Connection A data_version: {a_after_b}  (+{a_after_b - a_after_a})")
        print(f"  Connection B data_version: {b_after_b}  (+{b_after_b - b_after_a})")
        print()

        cross_a = b_after_a - b_before
        cross_b = a_after_b - a_after_a

        print("-" * 60)
        print("Cross-connection detection:")
        print(f"  A's write seen by B: {'DETECTED ✓' if cross_a > 0 else 'MISSED ✗'}")
        print(f"  B's write seen by A: {'DETECTED ✓' if cross_b > 0 else 'MISSED ✗'}")
        print()
        print("CONCLUSION:")
        if cross_a > 0 and cross_b > 0:
            print("  PRAGMA data_version is GLOBAL — any connection sees all commits.")
            print("  This is the mechanism honker uses for cross-process wake.")
        else:
            print("  Unexpected result — see deltas above.")

        conn_a.close()
        conn_b.close()


if __name__ == "__main__":
    main()
