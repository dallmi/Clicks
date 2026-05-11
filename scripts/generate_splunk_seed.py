#!/usr/bin/env python3
"""
Synthetic Splunk seed-data generator.

Emulates the shape of a Splunk SharePoint audit export:

    time,SiteUrl,Operation,ObjectId,UserId

The real Splunk export lives in a Corp environment and never enters this
repo. This script produces a structurally identical CSV so that
`compare_splunk.py` can be developed and tested end-to-end.

Strategy: read distinct CP_Link_address values (the click targets) and
dates from the local DuckDB (`data/clicks.db`) and emit synthetic
audit rows where `ObjectId` is derived from the same link address.
This mirrors the production semantics — Splunk's ObjectId and
AppInsights' CP_Link_address both identify the clicked target — and
guarantees the `--url-contains` filter hits matching rows on both
sides of the comparison.

Usage:
    python scripts/generate_splunk_seed.py
    python scripts/generate_splunk_seed.py --out input/splunk_seed.csv --noise 0.3
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "clicks.db"
DEFAULT_OUT = ROOT / "input" / "splunk_seed.csv"

OPERATIONS = [
    ("FileAccessed", 0.55),
    ("FileDownloaded", 0.15),
    ("FilePreviewed", 0.10),
    ("PageViewed", 0.15),
    ("FileModified", 0.05),
]
SITE_BASE = "https://corp.sharepoint.example/sites"
USER_POOL = [f"user{n:03d}@example.com" for n in range(1, 80)]


def weighted_choice(items: list[tuple[str, float]]) -> str:
    r = random.random()
    cum = 0.0
    for value, weight in items:
        cum += weight
        if r <= cum:
            return value
    return items[-1][0]


def derive_site_url(link_address: str) -> str:
    parsed = urlparse(link_address)
    parts = [p for p in parsed.path.split("/") if p]
    site_segment = parts[0] if parts else "general"
    return f"{SITE_BASE}/{site_segment}"


def derive_object_id(link_address: str) -> str:
    parsed = urlparse(link_address)
    path = parsed.path.lstrip("/") or "index"
    if "#" in link_address:
        path = path.split("#", 1)[0]
    return f"{SITE_BASE}/{path}/document_{random.randint(100, 999)}.pdf"


def generate(out_path: Path, noise_ratio: float, seed: int) -> None:
    random.seed(seed)

    if not DB_PATH.exists():
        raise SystemExit(f"DuckDB not found at {DB_PATH}. Run process_clicks.py first.")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(
        """
        SELECT CP_Link_address, session_date, COUNT(*) AS clicks
        FROM events
        WHERE CP_Link_address IS NOT NULL AND session_date IS NOT NULL
        GROUP BY 1, 2
        """
    ).fetchall()
    con.close()

    if not rows:
        raise SystemExit("No events in DB to base seed data on.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["time", "SiteUrl", "Operation", "ObjectId", "UserId"])

        for link_address, day, clicks in rows:
            # Splunk count drifts from AI clicks: ~60-110% of click volume.
            # Produces a realistic non-zero delta in the comparison.
            ratio = random.uniform(0.6, 1.1)
            count = max(0, int(clicks * ratio))
            site_url = derive_site_url(link_address)
            for _ in range(count):
                ts = datetime.combine(day, datetime.min.time()) + timedelta(
                    seconds=random.randint(0, 86_399)
                )
                writer.writerow(
                    [
                        ts.strftime("%d.%m.%Y %H:%M"),
                        site_url,
                        weighted_choice(OPERATIONS),
                        derive_object_id(link_address),
                        random.choice(USER_POOL),
                    ]
                )
                written += 1

        # Noise: unrelated audit events that should NOT be matched by
        # any plausible page-url substring. Keeps the filter honest.
        noise_count = int(written * noise_ratio)
        all_days = sorted({d for _, d, _ in rows})
        for _ in range(noise_count):
            day = random.choice(all_days)
            ts = datetime.combine(day, datetime.min.time()) + timedelta(
                seconds=random.randint(0, 86_399)
            )
            writer.writerow(
                [
                    ts.strftime("%d.%m.%Y %H:%M"),
                    f"{SITE_BASE}/misc",
                    weighted_choice(OPERATIONS),
                    f"{SITE_BASE}/misc/unrelated_{random.randint(1000, 9999)}.pdf",
                    random.choice(USER_POOL),
                ]
            )

    total = written + int(written * noise_ratio)
    print(f"Wrote {total:,} rows to {out_path} (matched: {written:,}, noise: {int(written * noise_ratio):,})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--noise", type=float, default=0.25, help="Fraction of unrelated rows to add")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(args.out, args.noise, args.seed)


if __name__ == "__main__":
    main()
