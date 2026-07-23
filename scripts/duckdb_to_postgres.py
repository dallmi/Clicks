"""
Migrates the Clicks DuckDB + CDM parquet star-schema into a Postgres database.

Output schema (default: "clicks"):

  Dimensions (PK = *_key):
    dim_date, dim_organization, dim_site, dim_page, dim_link_type,
    dim_component, dim_topic, dim_theme, dim_target_org, dim_target_region

  Fact:
    fact_clicks (FKs to all dims, plus a synthetic BIGSERIAL PK)

  Convenience:
    events_flat  -- the fully-enriched flat table from data/clicks.db (events)

Usage (default: fully embedded — no Postgres install needed):
  python scripts/duckdb_to_postgres.py
  python scripts/duckdb_to_postgres.py --drop

The script will:
  - auto-install missing pip dependencies (duckdb, psycopg[binary], pgserver)
  - launch an embedded Postgres server in data/postgres/ (binaries are
    downloaded by pgserver into the user cache on first run; no admin rights)
  - CREATE DATABASE if it doesn't exist
  - CREATE SCHEMA, all tables, PKs, FKs and indexes
  - bulk-load all data via COPY
  - leave the server running and print the connection URI for pgAdmin

To use an existing external Postgres instead:
  python scripts/duckdb_to_postgres.py --external
  PG* env vars (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE) are respected.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def _ensure_dependencies(extra: list[tuple[str, str]] | None = None) -> None:
    """Install missing pip packages so the user only needs to run the script."""
    required = [("duckdb", "duckdb"), ("psycopg", "psycopg[binary]")]
    if extra:
        required.extend(extra)
    missing = []
    for module, pip_name in required:
        try:
            __import__(module)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print(f"[migrate] Installing missing dependencies: {', '.join(missing)}", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", *missing])


# Embedded mode is the default — install pgserver too unless the user opted out.
_EMBEDDED = "--external" not in sys.argv
_ensure_dependencies(extra=[("pgserver", "pgserver")] if _EMBEDDED else None)

import duckdb  # noqa: E402
import psycopg  # noqa: E402
from psycopg import sql  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = ROOT / "data" / "clicks.db"
CDM_DIR = ROOT / "output" / "cdm"
EMBEDDED_PGDATA = ROOT / "data" / "postgres"
VIEWS_SQL = Path(__file__).resolve().parent / "analytics_views.sql"

DIMENSIONS = [
    # (parquet_name, table_name, pk_column, columns_ddl)
    ("dim_date", "dim_date", "date_key", """
        date_key      INTEGER PRIMARY KEY,
        date_value    DATE NOT NULL,
        year          INTEGER,
        quarter       INTEGER,
        month         INTEGER,
        month_name    TEXT,
        week          INTEGER,
        day_of_week   INTEGER,
        day_name      TEXT,
        is_weekend    BOOLEAN
    """),
    ("dim_organization", "dim_organization", "org_key", """
        org_key          INTEGER PRIMARY KEY,
        org_hash         TEXT,
        division         TEXT,
        unit             TEXT,
        area             TEXT,
        sector           TEXT,
        segment          TEXT,
        function         TEXT,
        ou_code          TEXT,
        country          TEXT,
        region           TEXT,
        job_title        TEXT,
        job_family       TEXT,
        management_level TEXT,
        cost_center      TEXT
    """),
    ("dim_site", "dim_site", "site_key", """
        site_key  INTEGER PRIMARY KEY,
        site_id   TEXT,
        site_name TEXT
    """),
    ("dim_page", "dim_page", "page_key", """
        page_key     INTEGER PRIMARY KEY,
        page_id      TEXT,
        page_name    TEXT,
        page_url     TEXT,
        content_type TEXT,
        page_status  TEXT
    """),
    ("dim_link_type", "dim_link_type", "link_type_key", """
        link_type_key INTEGER PRIMARY KEY,
        link_type     TEXT
    """),
    ("dim_component", "dim_component", "component_key", """
        component_key  INTEGER PRIMARY KEY,
        component_name TEXT
    """),
    ("dim_topic", "dim_topic", "topic_key", """
        topic_key INTEGER PRIMARY KEY,
        topic     TEXT
    """),
    ("dim_theme", "dim_theme", "theme_key", """
        theme_key INTEGER PRIMARY KEY,
        theme     TEXT
    """),
    ("dim_target_org", "dim_target_org", "target_org_key", """
        target_org_key INTEGER PRIMARY KEY,
        target_org     TEXT
    """),
    ("dim_target_region", "dim_target_region", "target_region_key", """
        target_region_key INTEGER PRIMARY KEY,
        target_region     TEXT
    """),
]

FACT_DDL = """
    fact_id                 BIGSERIAL PRIMARY KEY,
    date_key                INTEGER NOT NULL REFERENCES {schema}.dim_date(date_key),
    org_key                 INTEGER REFERENCES {schema}.dim_organization(org_key),
    site_key                INTEGER REFERENCES {schema}.dim_site(site_key),
    page_key                INTEGER REFERENCES {schema}.dim_page(page_key),
    link_type_key           INTEGER REFERENCES {schema}.dim_link_type(link_type_key),
    component_key           INTEGER REFERENCES {schema}.dim_component(component_key),
    topic_key               INTEGER REFERENCES {schema}.dim_topic(topic_key),
    theme_key               INTEGER REFERENCES {schema}.dim_theme(theme_key),
    target_org_key          INTEGER REFERENCES {schema}.dim_target_org(target_org_key),
    target_region_key       INTEGER REFERENCES {schema}.dim_target_region(target_region_key),
    person_hash             TEXT,
    user_id                 TEXT,
    session_id              TEXT,
    session_key             TEXT,
    "timestamp"             TIMESTAMP,
    timestamp_cet           TIMESTAMP,
    event_order             INTEGER,
    prev_event              TEXT,
    ms_since_prev_event     BIGINT,
    sec_since_prev_event    DOUBLE PRECISION,
    time_since_prev_bucket  TEXT,
    event_hour              INTEGER,
    event_weekday           TEXT,
    link_address            TEXT,
    link_label              TEXT,
    file_name               TEXT,
    file_type               TEXT,
    event_name              TEXT,
    client_country          TEXT
"""

FACT_COLUMNS = [
    "date_key", "org_key", "site_key", "page_key", "link_type_key",
    "component_key", "topic_key", "theme_key", "target_org_key", "target_region_key",
    "person_hash", "user_id", "session_id", "session_key",
    "timestamp", "timestamp_cet", "event_order", "prev_event",
    "ms_since_prev_event", "sec_since_prev_event", "time_since_prev_bucket",
    "event_hour", "event_weekday", "link_address", "link_label",
    "file_name", "file_type", "event_name", "client_country",
]

FACT_INDEXES = [
    ("ix_fact_clicks_date",       "(date_key)"),
    ("ix_fact_clicks_page",       "(page_key)"),
    ("ix_fact_clicks_org",        "(org_key)"),
    ("ix_fact_clicks_person",     "(person_hash)"),
    ("ix_fact_clicks_session",    "(session_key)"),
    ("ix_fact_clicks_timestamp",  "(\"timestamp\")"),
]


def log(msg: str) -> None:
    print(f"[migrate] {msg}", flush=True)


def get_pg_conn(args, dbname: str | None = None, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=dbname or args.dbname,
        autocommit=autocommit,
    )


def ensure_database(args) -> None:
    """Create the target Postgres database if it does not yet exist.

    CREATE DATABASE cannot run inside a transaction, so we connect to the
    server-level 'postgres' database with autocommit=True.
    """
    with get_pg_conn(args, dbname="postgres", autocommit=True) as pg, pg.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (args.dbname,))
        if cur.fetchone():
            log(f"Database '{args.dbname}' already exists")
            return
        log(f"CREATE DATABASE {args.dbname}")
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.dbname)))


def reset_schema(cur, schema: str, drop: bool) -> None:
    if drop:
        log(f"DROP SCHEMA {schema} CASCADE")
        cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
    log(f"CREATE SCHEMA {schema}")
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def create_dimension_tables(cur, schema: str) -> None:
    for _, table, _, ddl in DIMENSIONS:
        log(f"CREATE TABLE {schema}.{table}")
        cur.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(ddl),
        ))


def create_fact_table(cur, schema: str) -> None:
    log(f"CREATE TABLE {schema}.fact_clicks")
    cur.execute(sql.SQL("CREATE TABLE {}.fact_clicks ({})").format(
        sql.Identifier(schema),
        sql.SQL(FACT_DDL.format(schema=schema)),
    ))
    for ix_name, expr in FACT_INDEXES:
        cur.execute(sql.SQL("CREATE INDEX {} ON {}.fact_clicks {}").format(
            sql.Identifier(ix_name),
            sql.Identifier(schema),
            sql.SQL(expr),
        ))


def copy_arrow_to_postgres(
    cur,
    schema: str,
    table: str,
    columns: list[str],
    rows_iter,
) -> int:
    """Stream rows into Postgres using COPY ... FROM STDIN (binary-safe via text mode)."""
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        cols_sql,
    )
    n = 0
    with cur.copy(copy_sql) as copy:
        for row in rows_iter:
            copy.write_row(row)
            n += 1
    return n


def load_dimensions(duck: duckdb.DuckDBPyConnection, cur, schema: str) -> None:
    for parquet_name, table, _, _ in DIMENSIONS:
        path = CDM_DIR / f"{parquet_name}.parquet"
        if not path.exists():
            log(f"SKIP {table} (missing {path.name})")
            continue
        # Get column order from Postgres table to match parquet to columns
        cur.execute(sql.SQL("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """), (schema, table))
        cols = [r[0] for r in cur.fetchall()]
        rows = duck.execute(
            f"SELECT {', '.join(cols)} FROM read_parquet(?)", [str(path)]
        ).fetchall()
        n = copy_arrow_to_postgres(cur, schema, table, cols, rows)
        log(f"  loaded {n:,} rows into {schema}.{table}")


def load_fact(duck: duckdb.DuckDBPyConnection, cur, schema: str) -> None:
    path = CDM_DIR / "fact_clicks.parquet"
    if not path.exists():
        log(f"SKIP fact_clicks (missing {path.name})")
        return
    quoted = ", ".join(f'"{c}"' for c in FACT_COLUMNS)
    rows = duck.execute(
        f"SELECT {quoted} FROM read_parquet(?)", [str(path)]
    ).fetchall()
    n = copy_arrow_to_postgres(cur, schema, "fact_clicks", FACT_COLUMNS, rows)
    log(f"  loaded {n:,} rows into {schema}.fact_clicks")


def load_events_flat(duck: duckdb.DuckDBPyConnection, cur, schema: str) -> None:
    """Mirror the enriched DuckDB `events` table into Postgres for ad-hoc analysis."""
    log("Building events_flat from data/clicks.db")
    cols_info = duck.execute("DESCRIBE events").fetchall()
    type_map = {
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_NS": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
        "DATE": "DATE",
        "BIGINT": "BIGINT",
        "INTEGER": "INTEGER",
        "DOUBLE": "DOUBLE PRECISION",
        "BOOLEAN": "BOOLEAN",
        "VARCHAR": "TEXT",
    }
    ddl_cols = []
    col_names = []
    for col_name, col_type, *_ in cols_info:
        pg_type = type_map.get(col_type.upper(), "TEXT")
        ddl_cols.append(f'"{col_name}" {pg_type}')
        col_names.append(col_name)

    cur.execute(sql.SQL("CREATE TABLE {}.events_flat ({})").format(
        sql.Identifier(schema),
        sql.SQL(", ".join(ddl_cols)),
    ))
    # Useful indexes for the flat table
    cur.execute(sql.SQL(
        'CREATE INDEX ix_events_flat_session_date ON {}.events_flat (session_date)'
    ).format(sql.Identifier(schema)))
    cur.execute(sql.SQL(
        'CREATE INDEX ix_events_flat_gpn ON {}.events_flat (gpn)'
    ).format(sql.Identifier(schema)))

    # No backslash inside f-string expressions — Python < 3.12 rejects it.
    quoted_cols = ", ".join('"' + c + '"' for c in col_names)
    rows = duck.execute(f"SELECT {quoted_cols} FROM events").fetchall()
    n = copy_arrow_to_postgres(cur, schema, "events_flat", col_names, rows)
    log(f"  loaded {n:,} rows into {schema}.events_flat")


def _read_postmaster_pid() -> int | None:
    pid_file = EMBEDDED_PGDATA / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        first = pid_file.read_text().splitlines()[0].strip()
        return int(first)
    except (ValueError, IndexError):
        return None


def _read_postmaster_conn() -> tuple[str, int] | None:
    """Read (host, port) from postmaster.pid (line 4 = port, line 6 = listen addr)."""
    pid_file = EMBEDDED_PGDATA / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        lines = pid_file.read_text().splitlines()
        port = int(lines[3].strip())
        host = lines[5].strip() or "localhost"
        if host == "*":
            host = "127.0.0.1"
        return host, port
    except (ValueError, IndexError):
        return None


def _port_is_open(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _pid_is_postgres(pid: int) -> bool:
    """True only if `pid` is a live process AND it is postgres."""
    try:
        if os.name == "nt":
            out = subprocess.run(["tasklist", "/FI", "PID eq " + str(pid)],
                                 capture_output=True, text=True, timeout=10).stdout
            return "postgres" in out.lower()
        os.kill(pid, 0)  # POSIX only: signal 0 = existence check
        out = subprocess.run(["ps", "-p", str(pid), "-o", "comm="],
                             capture_output=True, text=True, timeout=10).stdout
        return "postgres" in out.lower()
    except (OSError, subprocess.SubprocessError):
        return False


def _clear_stale_postmaster() -> None:
    """Remove a stale postmaster.pid left behind by an unclean shutdown.

    pgserver only checks that the recorded PID exists; Windows recycles PIDs,
    so after a crash/Ctrl-C it can mistake an unrelated process for the
    server, report "ready" and never actually start Postgres. Consider the
    file stale when nothing listens on the recorded port AND the recorded
    PID is not a postgres process.
    """
    pid_file = EMBEDDED_PGDATA / "postmaster.pid"
    if not pid_file.exists():
        return
    conn = _read_postmaster_conn()
    if conn is not None and _port_is_open(conn[0], conn[1]):
        return  # server genuinely running
    pid = _read_postmaster_pid()
    if pid is not None and _pid_is_postgres(pid):
        log(f"postmaster.pid pid {pid} is a live postgres process; leaving it alone")
        return
    log(f"Removing STALE postmaster.pid (pid {pid} is not postgres, port closed)")
    pid_file.unlink()


def stop_embedded_postgres() -> int:
    """Stop the embedded Postgres server (kill postmaster from postmaster.pid)."""
    if not EMBEDDED_PGDATA.exists():
        log(f"No embedded Postgres data directory at {EMBEDDED_PGDATA}")
        return 0
    pid = _read_postmaster_pid()
    if pid is None:
        log("Embedded Postgres is not running (no postmaster.pid)")
        return 0
    log(f"Stopping embedded Postgres (pid {pid})")
    try:
        import pgserver  # type: ignore
        pgserver.get_server(str(EMBEDDED_PGDATA), cleanup_mode=None).cleanup()
        log("Stopped.")
        return 0
    except Exception as e:
        log(f"pgserver cleanup failed ({e}); falling back to OS signal")
        import signal
        try:
            os.kill(pid, signal.SIGTERM)
            log(f"Sent SIGTERM to pid {pid}")
            return 0
        except ProcessLookupError:
            log("Process already gone")
            return 0
        except Exception as e2:
            log(f"ERROR: could not stop server: {e2}")
            return 1


def status_embedded_postgres(args) -> int:
    pid = _read_postmaster_pid()
    if pid is None:
        log("Embedded Postgres: NOT running")
        log("Start it with: python scripts/duckdb_to_postgres.py --start")
        return 0
    conn = _read_postmaster_conn()
    if conn is None or not _port_is_open(conn[0], conn[1]):
        log(f"Embedded Postgres: NOT running — stale postmaster.pid (pid {pid}, port closed)")
        log("Start it with: python scripts/duckdb_to_postgres.py --start (cleans this up)")
        return 0
    log(f"Embedded Postgres: running (pid {pid}, data dir {EMBEDDED_PGDATA})")
    if conn is not None:
        # The embedded server uses trust auth (no password) and, on Windows,
        # a dynamically chosen port that changes on every restart — always
        # connect with the values shown here, not a remembered port.
        args.host, args.port = conn
        args.user = "postgres"
        args.password = ""
        _print_connection_banner(args)
    return 0


def start_embedded_postgres(args) -> "object":
    """Launch a local embedded Postgres via pgserver and patch args with its URI."""
    import pgserver  # type: ignore
    from urllib.parse import urlparse, unquote

    EMBEDDED_PGDATA.mkdir(parents=True, exist_ok=True)
    _clear_stale_postmaster()
    log(f"Starting embedded Postgres in {EMBEDDED_PGDATA}")
    log("(first run downloads ~50MB of Postgres binaries into the user cache)")
    # cleanup_mode=None -> server stays running after Python exits, so pgAdmin
    # can connect to it.
    try:
        server = pgserver.get_server(str(EMBEDDED_PGDATA), cleanup_mode=None)
    except Exception as e:
        # pgserver waits at most 10s for pg_ctl. After a crash the cluster
        # replays WAL first ("database system was interrupted" in the log),
        # which takes far longer on the network share — but the postmaster
        # keeps running. Poll until it accepts real logins.
        log(f"pgserver gave up waiting ({type(e).__name__}) — server is likely still")
        log("starting/recovering; polling up to 10 minutes ...")
        args.user = "postgres"
        args.password = ""
        deadline = time.time() + 600
        while True:
            conn = _read_postmaster_conn()
            if conn is not None and _port_is_open(conn[0], conn[1], timeout=5):
                try:
                    import psycopg
                    psycopg.connect(host=conn[0], port=conn[1], dbname="postgres",
                                    user=args.user, connect_timeout=10).close()
                    args.host, args.port = conn
                    log(f"Recovery finished — embedded Postgres ready at {args.host}:{args.port}")
                    return None
                except Exception as pe:
                    first_line = str(pe).strip().splitlines()[0] if str(pe).strip() else repr(pe)
                    log("  not accepting logins yet: " + first_line)
            if time.time() > deadline:
                log(f"ERROR: server did not come up within 10 minutes. Check {EMBEDDED_PGDATA / 'log'}")
                raise RuntimeError("embedded Postgres did not come up") from e
            time.sleep(10)

    uri = server.get_uri()
    parsed = urlparse(uri)
    args.host = parsed.hostname or "localhost"
    args.port = parsed.port or 5432
    args.user = unquote(parsed.username) if parsed.username else "postgres"
    args.password = unquote(parsed.password) if parsed.password else ""
    # Never trust "ready" — verify the port actually accepts connections.
    # (TCP URIs only; on POSIX pgserver uses a unix socket and has no port.)
    if parsed.port is None:
        log(f"Embedded Postgres ready (unix socket, user={args.user})")
    elif _port_is_open(args.host, args.port, timeout=10):
        log(f"Embedded Postgres ready at {args.host}:{args.port} (user={args.user}) — port verified")
    else:
        log(f"ERROR: pgserver reports ready, but {args.host}:{args.port} does not accept connections.")
        log(f"Check the server log: {EMBEDDED_PGDATA / 'log'}")
        raise RuntimeError("embedded Postgres did not come up")
    return server


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--external", action="store_true",
                        help="Use an external Postgres instead of the embedded server.")
    parser.add_argument("--start", action="store_true",
                        help="Just start the embedded Postgres server (no data migration) and exit.")
    parser.add_argument("--stop", action="store_true",
                        help="Stop the embedded Postgres server and exit.")
    parser.add_argument("--status", action="store_true",
                        help="Show whether the embedded Postgres server is running and exit.")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", "postgres"))
    parser.add_argument("--dbname", default=os.environ.get("PGDATABASE", "clicks"))
    parser.add_argument("--schema", default="clicks")
    parser.add_argument("--drop", action="store_true",
                        help="Drop and recreate the target schema (data loss).")
    parser.add_argument("--skip-flat", action="store_true",
                        help="Skip the events_flat table (CDM only).")
    args = parser.parse_args()

    if args.stop:
        return stop_embedded_postgres()
    if args.status:
        return status_embedded_postgres(args)
    if args.start:
        start_embedded_postgres(args)
        _print_connection_banner(args)
        return 0

    if not DUCKDB_PATH.exists():
        log(f"ERROR: {DUCKDB_PATH} not found")
        return 1
    if not CDM_DIR.exists():
        log(f"ERROR: {CDM_DIR} not found — run scripts/process_clicks.py first")
        return 1

    log(f"Source DuckDB: {DUCKDB_PATH}")
    log(f"Source CDM:    {CDM_DIR}")

    server = None
    if not args.external:
        server = start_embedded_postgres(args)

    log(f"Target Postgres: {args.user}@{args.host}:{args.port}/{args.dbname} schema={args.schema}")

    ensure_database(args)

    duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    with get_pg_conn(args) as pg, pg.cursor() as cur:
        reset_schema(cur, args.schema, args.drop)
        create_dimension_tables(cur, args.schema)
        create_fact_table(cur, args.schema)
        load_dimensions(duck, cur, args.schema)
        load_fact(duck, cur, args.schema)
        if not args.skip_flat:
            load_events_flat(duck, cur, args.schema)
        if VIEWS_SQL.exists():
            log(f"Creating analytics views from {VIEWS_SQL.name}")
            cur.execute(VIEWS_SQL.read_text(encoding="utf-8"))
        log("ANALYZE")
        cur.execute(sql.SQL("ANALYZE"))
        pg.commit()

    log("Done.")

    if server is not None:
        _print_connection_banner(args)
    return 0


def _print_connection_banner(args) -> None:
    print()
    print("=" * 70)
    print("  Embedded Postgres is running. Connect pgAdmin with:")
    print(f"    Host:     {args.host}")
    print(f"    Port:     {args.port}")
    print(f"    Database: {args.dbname}")
    print(f"    User:     {args.user}")
    print(f"    Password: {args.password or '(none — leave blank)'}")
    print()
    print("  pgAdmin prompts for a password on every connect unless you tick")
    print("  'Save password' — leave the field blank and tick it (trust auth).")
    print("  NOTE: the port changes on every server restart; re-check it here")
    print("  (--status) and update the pgAdmin server registration if needed.")
    print()
    print(f"  Data directory: {EMBEDDED_PGDATA}")
    print()
    print("  Start server:   python scripts\\duckdb_to_postgres.py --start")
    print("  Stop server:    python scripts\\duckdb_to_postgres.py --stop")
    print("  Server status:  python scripts\\duckdb_to_postgres.py --status")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
