#!/usr/bin/env python3
"""
Compare click volume between Application Insights (DuckDB) and Splunk
(CSV export) for a URL substring.

AppInsights side : data/clicks.db, table `events`, filter on CP_Link_address
                   (the click target, semantically equivalent to Splunk ObjectId)
Splunk side      : CSV with columns time, SiteUrl, Operation, ObjectId, UserId
                   filter on ObjectId

URLs are normalised on both sides via sharepoint_url.normalize() before
matching, so percent-encoded paths (`Shared%20Documents`), Office Online
short-link prefixes (`/:b:/r/...`), `Doc.aspx?file=...` wrappers and host
case differences no longer cause false negatives.

Both sides are aggregated per day and joined to produce a delta table.

The URL substring is supplied only via CLI and never persisted in the
repo. The Splunk CSV is expected to live outside version control.

Usage:
    python scripts/compare_splunk.py --url-contains "wellness"
    python scripts/compare_splunk.py --url-contains "wellness" --splunk input/splunk_seed.csv
    python scripts/compare_splunk.py --url-contains "wellness" --output output/compare.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys_path_added = str(Path(__file__).resolve().parent)
if sys_path_added not in sys.path:
    sys.path.insert(0, sys_path_added)
from sharepoint_url import normalize as normalize_sp_url  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "clicks.db"
DEFAULT_SPLUNK = ROOT / "input" / "splunk_seed.csv"
DEFAULT_OUT = ROOT / "output" / "compare_splunk.csv"


def compare(url_contains: str, splunk_csv: Path, db_path: Path, out_csv: Path) -> None:
    if not db_path.exists():
        sys.exit(f"DuckDB not found at {db_path}")
    if not splunk_csv.exists():
        sys.exit(f"Splunk CSV not found at {splunk_csv}")

    needle = (normalize_sp_url(url_contains) or url_contains).lower()
    needle = needle.replace("'", "''")
    con = duckdb.connect(":memory:")
    con.create_function(
        "sp_normalize",
        lambda u: normalize_sp_url(u),
        ["VARCHAR"],
        "VARCHAR",
        null_handling="special",
    )
    con.execute(f"ATTACH '{db_path}' AS clicks (READ_ONLY)")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW ai_daily AS
        SELECT session_date AS date, COUNT(*) AS clicks_ai
        FROM clicks.events
        WHERE CP_Link_address IS NOT NULL
          AND lower(sp_normalize(CP_Link_address)) LIKE '%{needle}%'
        GROUP BY 1
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW splunk_raw AS
        SELECT
            strptime("time", '%d.%m.%Y %H:%M') AS ts,
            "SiteUrl" AS site_url,
            "Operation" AS operation,
            "ObjectId" AS object_id,
            "UserId" AS user_id
        FROM read_csv_auto('{splunk_csv.as_posix()}', header=True, all_varchar=True)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW splunk_filtered AS
        SELECT
            CAST(ts AS DATE) AS date,
            user_id,
            sp_normalize(object_id) AS object_id,
            operation,
            date_trunc('minute', ts) AS ts_minute
        FROM splunk_raw
        WHERE object_id IS NOT NULL
          AND lower(sp_normalize(object_id)) LIKE '%{needle}%'
        """
    )

    # Dedupe: collapse FileAccessed + FileDownloaded (and any other ops) on
    # the same user+object within the same minute into one logical action.
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW splunk_daily AS
        WITH deduped AS (
            SELECT DISTINCT date, user_id, object_id, ts_minute
            FROM splunk_filtered
        )
        SELECT date, COUNT(*) AS clicks_splunk_dedup
        FROM deduped
        GROUP BY 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW splunk_breakdown AS
        SELECT
            date,
            SUM(CASE WHEN operation = 'FileAccessed'   THEN 1 ELSE 0 END) AS sp_accessed,
            SUM(CASE WHEN operation = 'FileDownloaded' THEN 1 ELSE 0 END) AS sp_downloaded,
            SUM(CASE WHEN operation NOT IN ('FileAccessed', 'FileDownloaded') THEN 1 ELSE 0 END) AS sp_other,
            COUNT(*) AS sp_raw_total
        FROM splunk_filtered
        GROUP BY 1
        """
    )

    compare_sql = """
        SELECT
            COALESCE(a.date, s.date, b.date)             AS date,
            COALESCE(a.clicks_ai, 0)                     AS clicks_ai,
            COALESCE(s.clicks_splunk_dedup, 0)           AS clicks_splunk,
            COALESCE(s.clicks_splunk_dedup, 0) - COALESCE(a.clicks_ai, 0) AS delta_abs,
            CASE
                WHEN COALESCE(a.clicks_ai, 0) = 0 THEN NULL
                ELSE ROUND(
                    100.0 * (COALESCE(s.clicks_splunk_dedup, 0) - COALESCE(a.clicks_ai, 0))
                    / COALESCE(a.clicks_ai, 0),
                    2
                )
            END                                          AS delta_pct,
            COALESCE(b.sp_accessed, 0)                   AS sp_accessed,
            COALESCE(b.sp_downloaded, 0)                 AS sp_downloaded,
            COALESCE(b.sp_other, 0)                      AS sp_other,
            COALESCE(b.sp_raw_total, 0)                  AS sp_raw_total
        FROM ai_daily a
        FULL OUTER JOIN splunk_daily s     USING (date)
        FULL OUTER JOIN splunk_breakdown b USING (date)
        ORDER BY date
    """
    result = con.execute(compare_sql).fetchall()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({compare_sql}) TO '{out_csv.as_posix()}' (HEADER, DELIMITER ',')")

    total_ai = sum(r[1] for r in result)
    total_splunk = sum(r[2] for r in result)
    total_delta = total_splunk - total_ai
    total_pct = (100.0 * total_delta / total_ai) if total_ai else None
    total_acc = sum(r[5] for r in result)
    total_dl = sum(r[6] for r in result)
    total_oth = sum(r[7] for r in result)
    total_raw = sum(r[8] for r in result)

    print(f"URL filter (ObjectId / CP_Link_address contains, case-insensitive): {url_contains!r}")
    print(f"Splunk CSV: {splunk_csv}")
    print(f"clicks_splunk = deduped per user+ObjectId+minute (collapses FileAccessed+FileDownloaded)")
    print(f"Days with activity: {len(result)}")
    print()
    header = (
        f"{'date':<12} {'clicks_ai':>10} {'splunk':>8} {'delta':>8} {'delta_%':>9} "
        f"{'accessed':>9} {'download':>9} {'other':>7} {'raw_tot':>8}"
    )
    print(header)
    print("-" * len(header))
    for date, ai, sp, da, dp, acc, dl, oth, raw in result:
        dp_str = f"{dp:>8.2f}%" if dp is not None else "     n/a"
        print(
            f"{str(date):<12} {ai:>10,} {sp:>8,} {da:>+8,} {dp_str} "
            f"{acc:>9,} {dl:>9,} {oth:>7,} {raw:>8,}"
        )
    print("-" * len(header))
    tp_str = f"{total_pct:>8.2f}%" if total_pct is not None else "     n/a"
    print(
        f"{'TOTAL':<12} {total_ai:>10,} {total_splunk:>8,} {total_delta:>+8,} {tp_str} "
        f"{total_acc:>9,} {total_dl:>9,} {total_oth:>7,} {total_raw:>8,}"
    )
    print()
    print(f"Wrote {out_csv}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url-contains", required=True, help="Substring filter (case-insensitive)")
    ap.add_argument("--splunk", type=Path, default=DEFAULT_SPLUNK)
    ap.add_argument("--db", type=Path, default=DB_PATH)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    compare(args.url_contains, args.splunk, args.db, args.output)


if __name__ == "__main__":
    main()
