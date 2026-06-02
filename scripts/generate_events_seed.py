#!/usr/bin/env python3
"""
Synthetic dashboard seed-data generator.

Produces `output/events_raw.parquet` directly with the exact schema the
dashboards (`dashboard/dashboard.html`, `dashboard/dashboard2.html`) read,
so you can demo / visualise the dashboard without running the full
process_clicks.py pipeline (which needs real KQL exports + an external
hr_history.parquet).

Usage (from repo root):
    python scripts/generate_events_seed.py
    python scripts/generate_events_seed.py --rows 8000 --days 180 --seed 42
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "output" / "events_raw.parquet"

# ---- Canonical HR taxonomy (matches dashboard color maps) ------------------
DIVISIONS = [
    ("Investment Bank",              ["Global Markets", "Global Banking", "IB Research"]),
    ("Global Wealth Management",     ["Wealth Advisory", "Investment Solutions", "Client Strategy"]),
    ("Asset Management",             ["Equities", "Fixed Income", "Multi-Asset"]),
    ("Group Functions",              ["Finance", "Risk", "Technology", "HR"]),
    ("Personal & Corporate Banking", ["Retail", "Corporate Clients", "Mortgages"]),
]
DIVISION_WEIGHTS = [0.30, 0.25, 0.15, 0.18, 0.12]

AREAS_BY_UNIT = {
    "Global Markets":        ["Equities Trading", "Rates", "FX"],
    "Global Banking":        ["M&A", "Capital Markets", "Leveraged Finance"],
    "IB Research":           ["Research Macro", "Research Equity"],
    "Wealth Advisory":       ["UHNW Desk", "HNW Desk", "Private Clients"],
    "Investment Solutions":  ["Structured Products", "Funds Selection"],
    "Client Strategy":       ["Client Intelligence", "Segmentation"],
    "Equities":              ["Active Equities", "Passive Equities"],
    "Fixed Income":          ["Core FI", "EM Debt"],
    "Multi-Asset":           ["MA Allocation", "MA Solutions"],
    "Finance":               ["Controlling", "Accounting", "FP&A"],
    "Risk":                  ["Credit Risk", "Market Risk", "Op Risk"],
    "Technology":            ["Platform Eng", "Data & Analytics", "Cyber"],
    "HR":                    ["Talent", "Comp & Ben"],
    "Retail":                ["Branches", "Digital Banking"],
    "Corporate Clients":     ["SME", "Large Corp"],
    "Mortgages":             ["Direct Mortgages", "Broker Channel"],
}

SECTORS = ["North", "South", "East", "West", "Central"]

REGIONS = [
    ("SWITZERLAND", ["Switzerland"],                                            0.40),
    ("EMEA",        ["United Kingdom", "Germany", "France", "Italy", "Spain"], 0.30),
    ("AMERICAS",    ["United States", "Canada", "Brazil"],                     0.18),
    ("APAC",        ["Singapore", "Hong Kong", "Japan", "Australia"],          0.12),
]

# ---- Intranet content ------------------------------------------------------
SITES = [
    ("S0001", "HR Hub"),
    ("S0002", "Markets Insights"),
    ("S0003", "Tech Knowledge Base"),
    ("S0004", "Wealth News"),
    ("S0005", "Compliance Centre"),
]
SITE_WEIGHTS = [0.25, 0.25, 0.15, 0.20, 0.15]

PAGES_BY_SITE = {
    "HR Hub": [
        ("Benefits Overview",       "benefits-overview"),
        ("New Joiners Guide",       "new-joiners"),
        ("Annual Compensation",     "annual-comp"),
        ("Performance Cycle",       "perf-cycle"),
        ("Leave & Holidays",        "leave-holidays"),
        ("Learning Catalogue",      "learning"),
    ],
    "Markets Insights": [
        ("Daily Market Briefing",   "daily-briefing"),
        ("Equity Outlook 2026",     "equity-outlook"),
        ("Rates & FX Weekly",       "rates-fx"),
        ("Earnings Recap",          "earnings-recap"),
        ("Macro Themes",            "macro-themes"),
    ],
    "Tech Knowledge Base": [
        ("Cloud Migration Playbook","cloud-migration"),
        ("Cybersecurity Updates",   "cyber-updates"),
        ("Data Platform Roadmap",   "data-roadmap"),
        ("Developer Tooling",       "dev-tooling"),
    ],
    "Wealth News": [
        ("Client Strategy Quarterly","client-strategy-q"),
        ("Tax Planning 2026",       "tax-planning"),
        ("Portfolio Review Toolkit","portfolio-toolkit"),
        ("Sustainable Investing",   "sustainable-investing"),
    ],
    "Compliance Centre": [
        ("Trade Surveillance Rules","trade-surveillance"),
        ("Mandatory Training",      "mandatory-training"),
        ("Reporting Deadlines",     "reporting-deadlines"),
        ("Conduct Policies",        "conduct-policies"),
    ],
}

LINKS_PER_PAGE = [
    # (label, href_slug, optional file)
    ("Read more",                 "read-more",       None),
    ("Download PDF",              "download.pdf",    ("Briefing", "PDF")),
    ("Watch video",               "video",           None),
    ("Open spreadsheet",          "data.xlsx",       ("Data Sheet", "XLSX")),
    ("Slide deck",                "deck.pptx",       ("Slides", "PPTX")),
    ("Apply now",                 "apply",           None),
    ("Submit feedback",           "feedback",        None),
    ("Related article",           "related",         None),
    ("Policy document",           "policy.pdf",      ("Policy", "PDF")),
    ("Q&A document",              "qa.docx",         ("Q&A", "DOCX")),
]

TOPICS  = ["Strategy", "Operations", "Technology", "Compliance", "People", "Markets"]
THEMES  = ["Innovation", "Growth", "Risk", "Sustainability", "Client Centricity",
           "Efficiency", "Culture", "Digital", "Regulatory", "Talent"]
TARGET_ORGS    = ["Investment Bank", "Wealth Management", "All Staff", "Group Functions", "P&C Banking"]
TARGET_REGIONS = ["Global", "EMEA", "AMERICAS", "APAC", "Switzerland"]


def weighted_pick(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def build_users(n: int) -> list[dict]:
    """Synthetic user pool with stable HR attributes."""
    users = []
    for i in range(1, n + 1):
        gpn = f"{10_000_000 + i:08d}"
        division, units = weighted_pick(DIVISIONS, DIVISION_WEIGHTS)
        unit = random.choice(units)
        area = random.choice(AREAS_BY_UNIT[unit])
        sector = random.choice(SECTORS)
        region, countries, _ = weighted_pick(REGIONS, [w for *_, w in REGIONS])
        country = random.choice(countries)
        users.append({
            "gpn":         gpn,
            "hr_division": division,
            "hr_unit":     unit,
            "hr_area":     area,
            "hr_sector":   sector,
            "hr_region":   region,
            "hr_country":  country,
        })
    return users


def build_pages() -> list[dict]:
    pages = []
    for site_id, site_name in SITES:
        for page_name, slug in PAGES_BY_SITE[site_name]:
            page_url = f"https://intranet.example.com/{slug}"
            pages.append({
                "site_id":   site_id,
                "site_name": site_name,
                "page_name": page_name,
                "page_url":  page_url,
                "topic":     random.choice(TOPICS),
                "theme":     random.choice(THEMES),
                "target_org":    random.choice(TARGET_ORGS),
                "target_region": random.choice(TARGET_REGIONS),
            })
    return pages


def generate_events(rows: int, days: int, seed: int) -> pd.DataFrame:
    random.seed(seed)
    users  = build_users(120)
    pages  = build_pages()
    user_weights = [random.uniform(0.3, 3.0) for _ in users]   # power-law-ish
    page_weights = [random.uniform(0.3, 3.0) for _ in pages]

    end_dt   = datetime.now(timezone.utc).replace(microsecond=0)
    start_dt = end_dt - timedelta(days=days)
    span_s   = int((end_dt - start_dt).total_seconds())

    records = []
    for i in range(rows):
        u = random.choices(users, weights=user_weights, k=1)[0]
        p = random.choices(pages, weights=page_weights, k=1)[0]
        # Diurnal-ish bias: more clicks during business hours
        t_offset = int(random.triangular(0, span_s, span_s * 0.7))
        ts_utc = start_dt + timedelta(seconds=t_offset)

        link_label, link_slug, file_meta = random.choice(LINKS_PER_PAGE)
        link_address = f"{p['page_url']}/{link_slug}"
        if file_meta:
            file_name, file_type = file_meta
            # Make file_name varied by page
            file_name = f"{p['page_name']} – {file_name}"
        else:
            file_name = None
            file_type = None

        # CET-ish: just add +1h for the simple seed
        ts_cet = ts_utc + timedelta(hours=1)
        records.append({
            "timestamp":          ts_utc,
            "timestamp_cet":      ts_cet,
            "session_date":       ts_cet.date(),
            "name":               "click_event",
            "user_id":            f"u_{u['gpn']}",
            "session_id":         f"s_{u['gpn']}_{ts_cet.strftime('%Y%m%d')}_{random.randint(1,4)}",
            "CP_GPN":             u["gpn"],
            "gpn":                u["gpn"],
            "CP_SiteID":          p["site_id"],
            "CP_SiteName":        p["site_name"],
            "CP_PageName":        p["page_name"],
            "CP_PageURL":         p["page_url"],
            "CP_PageStatus":      "Published",
            "CP_Link_Type":       "external" if file_meta else random.choice(["internal", "external", "anchor"]),
            "CP_Link_label":      link_label,
            "CP_Link_address":    link_address,
            "CP_FileType_Label":  file_type,
            "CP_FileName_Label":  file_name,
            "CP_Topic":           p["topic"],
            "CP_Theme":           p["theme"],
            "CP_TargetOrg":       p["target_org"],
            "CP_TargetRegion":    p["target_region"],
            "hr_division":        u["hr_division"],
            "hr_unit":            u["hr_unit"],
            "hr_area":            u["hr_area"],
            "hr_sector":          u["hr_sector"],
            "hr_region":          u["hr_region"],
            "hr_country":         u["hr_country"],
        })

    df = pd.DataFrame(records)
    # Stable column order for nicer parquet
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=5000, help="Number of click events to generate")
    ap.add_argument("--days", type=int, default=180, help="Date range in days, ending today")
    ap.add_argument("--seed", type=int, default=42,   help="Random seed")
    ap.add_argument("--out",  type=Path, default=OUT_PATH, help="Output parquet path")
    args = ap.parse_args()

    print(f"Generating {args.rows:,} synthetic click events over {args.days} days (seed={args.seed})…")
    df = generate_events(args.rows, args.days, args.seed)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    # Write via DuckDB so the parquet is shaped identically to the real pipeline.
    con = duckdb.connect()
    con.register("seed_df", df)
    con.execute(f"COPY (SELECT * FROM seed_df) TO '{args.out.as_posix()}' (FORMAT 'parquet')")
    con.close()
    print(f"Wrote {args.out.relative_to(ROOT) if args.out.is_absolute() else args.out} "
          f"({args.out.stat().st_size / 1e6:.2f} MB, {len(df):,} rows)")


if __name__ == "__main__":
    main()
