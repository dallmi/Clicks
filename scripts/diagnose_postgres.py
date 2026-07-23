"""
One-shot diagnosis of the embedded Postgres setup (Windows work environment).

Runs every check in sequence, prints [OK]/[WARN]/[FAIL] per step, and ends
with a verdict plus the exact next command to run and the pgAdmin settings.
Read-only: starts nothing, changes nothing.

Usage:
  python scripts/diagnose_postgres.py
  python scripts/diagnose_postgres.py --host localhost --port 5432 --user micha
      (override target, e.g. to test an external Postgres instead)

Compatible with Python 3.8+ (the work environment runs < 3.12).
"""

from __future__ import annotations

import argparse
import os
import platform
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = ROOT / "data" / "clicks.db"
CDM_DIR = ROOT / "output" / "cdm"
EMBEDDED_PGDATA = ROOT / "data" / "postgres"
PG_LOG = EMBEDDED_PGDATA / "log"

PGADMIN_DEFAULT_TIMEOUT_S = 10  # pgAdmin aborts connects after this many seconds

_findings = []  # (level, message) collected for the final verdict


def report(level: str, msg: str) -> None:
    print("  [" + level + "] " + msg)
    _findings.append((level, msg))


def section(title: str) -> None:
    print()
    print("== " + title + " " + "=" * max(0, 60 - len(title)))


def is_network_path(p: Path) -> bool:
    s = str(p)
    if s.startswith("\\\\"):
        return True
    if os.name == "nt":
        try:
            import ctypes
            drive = os.path.splitdrive(s)[0]
            if drive:
                dtype = ctypes.windll.kernel32.GetDriveTypeW(drive + "\\")
                return dtype == 4  # DRIVE_REMOTE
        except Exception:
            pass
    return False


def read_postmaster(pgdata: Path):
    """Parse postmaster.pid -> dict with pid/port/host, or None."""
    pid_file = pgdata / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        lines = pid_file.read_text().splitlines()
        info = {"pid": int(lines[0].strip())}
        if len(lines) > 3 and lines[3].strip():
            info["port"] = int(lines[3].strip())
        if len(lines) > 5 and lines[5].strip():
            info["host"] = lines[5].strip()
        return info
    except (ValueError, IndexError, OSError) as e:
        return {"error": str(e)}


def tcp_check(host: str, port: int, timeout: float = 10.0):
    """Try a raw TCP connect. Returns (ok, seconds, error_message)."""
    t0 = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, time.time() - t0, ""
    except Exception as e:
        return False, time.time() - t0, str(e)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None, help="Override host (default: from postmaster.pid)")
    parser.add_argument("--port", type=int, default=None, help="Override port (default: from postmaster.pid)")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--dbname", default="clicks", help="Application database to look for")
    parser.add_argument("--timeout", type=int, default=30, help="Connect timeout in seconds")
    args = parser.parse_args()

    print("Embedded Postgres diagnosis — " + time.strftime("%Y-%m-%d %H:%M:%S"))
    print("Project root: " + str(ROOT))

    # -- 1. Python / platform ------------------------------------------------
    section("Python & platform")
    pyver = platform.python_version()
    report("OK", "Python " + pyver + " (" + sys.executable + ")")
    report("OK", "Platform: " + platform.system() + " " + platform.release())
    if sys.version_info < (3, 12):
        report("INFO", "Python < 3.12 — script code must avoid f-string backslashes (already handled)")

    # -- 2. Project files ----------------------------------------------------
    section("Project files")
    if DUCKDB_PATH.exists():
        size_mb = DUCKDB_PATH.stat().st_size / 1e6
        report("OK", "DuckDB source data/clicks.db present (%.1f MB)" % size_mb)
    else:
        report("FAIL", "data/clicks.db missing — nothing to migrate; run scripts/process_clicks.py first")
    if CDM_DIR.exists():
        n_parquet = len(list(CDM_DIR.glob("*.parquet")))
        report("OK", "output/cdm present (" + str(n_parquet) + " parquet files)")
    else:
        report("WARN", "output/cdm missing — full migration will refuse to run (scripts/process_clicks.py)")

    # -- 3. Embedded data directory ------------------------------------------
    section("Embedded Postgres data directory")
    server_host, server_port = args.host, args.port
    # With an explicit --host/--port target the embedded dir is informational only.
    explicit_target = args.host is not None or args.port is not None
    if not EMBEDDED_PGDATA.exists():
        report("INFO" if explicit_target else "FAIL",
               "data/postgres does not exist — embedded server was never initialized here")
    else:
        report("OK", "data/postgres exists")
        if is_network_path(EMBEDDED_PGDATA):
            report("WARN", "data dir is on a NETWORK share — expect slow connects; "
                           "raise pgAdmin Advanced > Connection timeout to 60")
        if (EMBEDDED_PGDATA / "PG_VERSION").exists():
            report("OK", "cluster initialized (PG_VERSION " +
                   (EMBEDDED_PGDATA / "PG_VERSION").read_text().strip() + ")")
        else:
            report("FAIL", "PG_VERSION missing — cluster init incomplete/corrupt; "
                           "delete data/postgres and re-run --start")

        pm = read_postmaster(EMBEDDED_PGDATA)
        if pm is None:
            report("INFO" if explicit_target else "FAIL",
                   "no postmaster.pid — embedded server is NOT running")
        elif "error" in pm:
            report("WARN", "postmaster.pid unreadable: " + pm["error"])
        else:
            report("OK", "postmaster.pid: pid=" + str(pm.get("pid")) +
                   " host=" + str(pm.get("host", "?")) + " port=" + str(pm.get("port", "?")))
            if server_host is None:
                server_host = pm.get("host", "127.0.0.1")
            if server_port is None:
                server_port = pm.get("port")

    # -- 4. Server log tail ---------------------------------------------------
    if PG_LOG.exists():
        section("Server log (last 10 lines of data/postgres/log)")
        try:
            tail = PG_LOG.read_text(errors="replace").splitlines()[-10:]
            for line in tail:
                print("    | " + line)
        except OSError as e:
            report("WARN", "could not read log: " + str(e))

    # -- 5. TCP reachability ---------------------------------------------------
    section("TCP connectivity")
    if server_port is None:
        report("FAIL", "no port known (server not running and no --port given) — skipping connect tests")
        server_reachable = False
    else:
        host = server_host or "127.0.0.1"
        ok, secs, err = tcp_check(host, server_port, timeout=args.timeout)
        server_reachable = ok
        if ok:
            report("OK", "TCP connect to " + host + ":" + str(server_port) + " in %.2fs" % secs)
            if secs > PGADMIN_DEFAULT_TIMEOUT_S - 2:
                report("WARN", "connect is close to pgAdmin's %ds default timeout — raise it to 60"
                       % PGADMIN_DEFAULT_TIMEOUT_S)
        else:
            report("FAIL", "TCP connect to " + host + ":" + str(server_port) +
                   " failed after %.1fs: " % secs + err)

    # -- 6. Postgres protocol ---------------------------------------------------
    clicks_exists = False
    if server_reachable:
        section("Postgres connection (user=" + args.user + ", trust auth)")
        try:
            import psycopg
        except ImportError:
            psycopg = None
            report("WARN", "psycopg not installed — run: pip install \"psycopg[binary]\"")
        if psycopg is not None:
            try:
                t0 = time.time()
                conn = psycopg.connect(host=host, port=server_port, dbname="postgres",
                                       user=args.user, connect_timeout=args.timeout)
                dt = time.time() - t0
                ver = conn.execute("SELECT version()").fetchone()[0].split(" on ")[0]
                report("OK", ver + " — connected in %.2fs" % dt)
                if dt > PGADMIN_DEFAULT_TIMEOUT_S - 2:
                    report("WARN", "login took %.1fs — pgAdmin aborts at %ds by default; "
                           "set Advanced > Connection timeout = 60" % (dt, PGADMIN_DEFAULT_TIMEOUT_S))
                rows = conn.execute("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY 1").fetchall()
                dbs = [r[0] for r in rows]
                report("OK", "databases: " + ", ".join(dbs))
                clicks_exists = args.dbname in dbs
                conn.close()
                if clicks_exists:
                    conn = psycopg.connect(host=host, port=server_port, dbname=args.dbname,
                                           user=args.user, connect_timeout=args.timeout)
                    n_tables = conn.execute(
                        "SELECT count(*) FROM information_schema.tables "
                        "WHERE table_schema = 'clicks'").fetchone()[0]
                    if n_tables:
                        n_facts = conn.execute("SELECT count(*) FROM clicks.fact_clicks").fetchone()[0]
                        report("OK", "database '" + args.dbname + "': " + str(n_tables) +
                               " tables/views in schema clicks, fact_clicks=" + format(n_facts, ","))
                    else:
                        report("WARN", "database '" + args.dbname + "' exists but schema clicks is empty "
                               "— run the migration")
                    conn.close()
                else:
                    report("WARN", "database '" + args.dbname + "' missing — run the migration")
            except Exception as e:
                report("FAIL", "Postgres login failed: " + str(e).strip())

    # -- Verdict -----------------------------------------------------------------
    section("VERDICT")
    fails = [m for lvl, m in _findings if lvl == "FAIL"]
    warns = [m for lvl, m in _findings if lvl == "WARN"]

    if not server_reachable:
        print("  Server is NOT reachable. Next step:")
        print()
        print("      python scripts" + os.sep + "duckdb_to_postgres.py --start")
        print()
        print("  Then run this diagnosis again. NOTE: the port changes on every")
        print("  server restart — always use the port this script reports.")
    elif not clicks_exists:
        print("  Server runs, but the '" + args.dbname + "' database is missing. Next step:")
        print()
        print("      python scripts" + os.sep + "duckdb_to_postgres.py")
        print()
        print("  (full migration: creates the database and loads all tables)")
    elif fails:
        print("  Problems found:")
        for m in fails:
            print("    - " + m)
    else:
        print("  Everything works. If pgAdmin still fails, the problem is pgAdmin-side:")
        print("    1. Advanced > Connection timeout = 60   (default 10s is too low")
        print("       for a data dir on a network share)")
        print("    2. Password: leave blank, tick 'Save password' (trust auth)")
        print("    3. If it STILL times out, corporate endpoint security is")
        print("       filtering localhost ports — report back for a workaround.")

    if server_port is not None and server_reachable:
        print()
        print("  pgAdmin settings (current, verified):")
        print("      Host:                 " + (server_host or "127.0.0.1"))
        print("      Port:                 " + str(server_port))
        print("      Maintenance database: postgres")
        print("      Username:             " + args.user)
        print("      Password:             (leave blank)")
        print("      Advanced > Connection timeout: 60")

    if warns:
        print()
        print("  Warnings:")
        for m in warns:
            print("    - " + m)

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
