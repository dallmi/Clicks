#!/usr/bin/env python3
"""
Generate a synthetic hr_history.parquet matching the GPNs present in
output/events_raw.parquet. Output goes to output/hr_history.parquet.

Schema mirrors what process_clicks.add_calculated_columns() expects:
  gpn, snapshot_year, snapshot_month,
  gcrs_division_desc, gcrs_unit_desc, gcrs_area_desc,
  gcrs_sector_desc, gcrs_segment_desc, gcrs_function_desc,
  ou_desc, ou_code,
  work_location_country, work_location_region

Deterministic: a given GPN always maps to the same HR record (hash-seeded).
"""

import hashlib
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENTS_PATH  = PROJECT_ROOT / 'output' / 'events_raw.parquet'
OUT_PATH     = PROJECT_ROOT / 'output' / 'hr_history.parquet'

# ---------- Org hierarchy: Division → Unit → Area → Sector → Segment → Function ----------
ORG: dict = {
    'Investment Bank': {
        'Global Markets': {
            'Equities': {
                'Cash Equities':     {'Trading': ['Execution', 'Market Making'], 'Sales': ['Inst. Sales', 'Hedge Fund Sales']},
                'Equity Derivatives':{'Flow': ['Index', 'Single Stock'], 'Structured': ['Exotics', 'Hybrids']},
                'Prime Brokerage':   {'Client Service': ['Onboarding'], 'Financing': ['Sec Lending']},
            },
            'FX Rates & Credit': {
                'FX':     {'Spot': ['G10', 'EM'], 'Options': ['Vanilla', 'Exotic']},
                'Rates':  {'Govt': ['Trading'], 'Swaps': ['Flow', 'Structured']},
                'Credit': {'IG': ['Trading'], 'HY': ['Trading'], 'CDS': ['Index']},
            },
        },
        'Global Banking': {
            'Advisory': {
                'M&A':      {'Healthcare': ['Pharma', 'MedTech'], 'TMT': ['Tech', 'Media']},
                'Restructuring': {'Special Situations': ['Distressed']},
            },
            'Capital Markets': {
                'ECM': {'Equity Origination': ['IPO', 'Follow-on']},
                'DCM': {'Debt Origination': ['IG', 'HY', 'Sovereigns']},
            },
        },
    },
    'Global Wealth Management': {
        'Switzerland & EMEA': {
            'Switzerland': {
                'UHNW': {'Family Office': ['CH-East', 'CH-West'], 'Entrepreneurs': ['CH']},
                'HNW':  {'Private Banking': ['Zurich', 'Geneva', 'Basel']},
            },
            'EMEA': {
                'UK & Channel Islands': {'UHNW': ['London'], 'HNW': ['London']},
                'Germany & Austria':    {'UHNW': ['Frankfurt'], 'HNW': ['Munich', 'Vienna']},
                'France & Benelux':     {'UHNW': ['Paris'],     'HNW': ['Paris', 'Brussels']},
            },
        },
        'Americas': {
            'United States': {
                'UHNW': {'Wealth Advisory': ['New York', 'Miami', 'San Francisco']},
                'HNW':  {'Wealth Advisory': ['Chicago', 'Boston']},
            },
            'Latin America': {
                'Brazil': {'Private Banking': ['São Paulo']},
                'Mexico': {'Private Banking': ['Mexico City']},
            },
        },
        'APAC': {
            'Greater China': {'UHNW': {'Wealth Advisory': ['Hong Kong', 'Shanghai']}},
            'South-East Asia': {'UHNW': {'Wealth Advisory': ['Singapore']}},
            'Japan & Australia': {'HNW': {'Wealth Advisory': ['Tokyo', 'Sydney']}},
        },
    },
    'Asset Management': {
        'Active Equities': {
            'Global Equities':  {'Quant': {'Research': ['Factor', 'Alpha']}, 'Fundamental': {'Research': ['Sector', 'Region']}},
            'Regional Equities':{'EMEA': {'Portfolio Mgmt': ['EU Large-Cap']}, 'APAC': {'Portfolio Mgmt': ['Asia Equities']}},
        },
        'Fixed Income': {
            'Global Rates':  {'Sovereigns': {'Portfolio Mgmt': ['Core', 'Plus']}, 'Inflation': {'Portfolio Mgmt': ['Linkers']}},
            'Credit':        {'IG Credit': {'Portfolio Mgmt': ['Global IG']}, 'HY Credit': {'Portfolio Mgmt': ['Global HY']}},
        },
        'Multi-Asset & Alternatives': {
            'Multi-Asset':   {'Solutions': {'Portfolio Mgmt': ['Balanced', 'Income']}},
            'Hedge Funds':   {'Liquid Alts': {'Portfolio Mgmt': ['Macro', 'L/S Equity']}},
            'Private Markets':{'Real Estate': {'Investment Mgmt': ['Core+', 'Value-Add']}},
        },
    },
    'Personal & Corporate Banking': {
        'Personal Banking': {
            'Retail': {
                'Branch Network':  {'Customer Advisory': ['Region North', 'Region South', 'Region East', 'Region West']},
                'Digital Banking': {'Mobile':            ['Product', 'Engineering']},
            },
            'Mortgages': {'Underwriting': {'Operations': ['Residential', 'Refi']}},
        },
        'Corporate Banking': {
            'Large Corporates': {'Relationship Mgmt': {'Sectors': ['Industrials', 'Energy', 'TMT']}},
            'SME Banking':      {'Relationship Mgmt': {'Regions': ['DACH', 'France', 'Italy']}},
        },
        'Wealth Solutions': {
            'Cards & Lending': {'Cards': {'Product': ['Premium', 'Mass-Affluent']}},
        },
    },
    'Group Functions': {
        'Technology': {
            'Engineering': {
                'Platform Engineering': {'Cloud':       ['SRE', 'Tooling']},
                'Application Engineering':{'Trading Tech': ['FX', 'Equities'], 'Wealth Tech': ['Advisory']},
            },
            'Cybersecurity': {'Defensive Security': {'SOC': ['Tier 1', 'Tier 2'], 'Threat Intel': ['Hunting']}},
            'Data & AI':     {'Data Platform': {'Engineering': ['Lakehouse', 'Streaming']}, 'AI/ML': {'Research': ['NLP', 'Forecasting']}},
        },
        'Risk':       {'Market Risk': {'Trading Risk': {'Reporting': ['EMEA', 'Americas']}}, 'Credit Risk': {'Counterparty': {'Reporting': ['Global']}}},
        'Compliance': {'Financial Crime': {'AML':           {'Investigations': ['EMEA', 'Americas']}, 'Sanctions': {'Screening': ['Global']}}},
        'Finance':    {'Controlling':     {'FP&A':           {'Reporting': ['Group', 'Divisional']}}},
        'Legal':      {'Corporate':       {'Securities Law': {'Advisory': ['Global']}}},
        'HR':         {'People Advisory': {'Talent':         {'Acquisition': ['EMEA', 'APAC']}}},
        'Marketing & Communications': {'Brand': {'Campaigns': {'Production': ['EMEA', 'APAC']}}},
    },
}

# ---------- Region → Country distribution (deterministic per gpn) ----------
REGION_COUNTRIES: dict = {
    'SWITZERLAND': ['Switzerland'],
    'EMEA':        ['Germany', 'United Kingdom', 'France', 'Italy', 'Spain', 'Poland', 'Netherlands', 'Sweden'],
    'AMERICAS':    ['United States', 'Canada', 'Brazil', 'Mexico', 'Argentina'],
    'APAC':        ['Singapore', 'Hong Kong', 'Japan', 'Australia', 'China', 'India'],
}

# Division weights (rough headcount distribution) and region weights per division.
DIVISION_WEIGHTS = [
    ('Global Wealth Management',     0.32),
    ('Investment Bank',              0.22),
    ('Personal & Corporate Banking', 0.20),
    ('Group Functions',              0.18),
    ('Asset Management',             0.08),
]

REGION_WEIGHTS_BY_DIVISION = {
    'Investment Bank':              [('SWITZERLAND', 0.18), ('EMEA', 0.42), ('AMERICAS', 0.28), ('APAC', 0.12)],
    'Global Wealth Management':     [('SWITZERLAND', 0.30), ('EMEA', 0.30), ('AMERICAS', 0.22), ('APAC', 0.18)],
    'Asset Management':             [('SWITZERLAND', 0.20), ('EMEA', 0.40), ('AMERICAS', 0.22), ('APAC', 0.18)],
    'Personal & Corporate Banking': [('SWITZERLAND', 0.55), ('EMEA', 0.30), ('AMERICAS', 0.10), ('APAC', 0.05)],
    'Group Functions':              [('SWITZERLAND', 0.45), ('EMEA', 0.25), ('AMERICAS', 0.18), ('APAC', 0.12)],
}


def stable_choice(seed_str: str, key: str, options):
    """Deterministic weighted/uniform choice. options can be list or list of (val, weight)."""
    h = hashlib.sha256(f'{seed_str}|{key}'.encode()).hexdigest()
    n = int(h[:12], 16)
    if not options:
        return None
    if isinstance(options[0], tuple):
        total = sum(w for _, w in options)
        r = (n / float(1 << 48)) * total
        cum = 0.0
        for val, w in options:
            cum += w
            if r <= cum:
                return val
        return options[-1][0]
    return options[n % len(options)]


def walk_org(seed: str, division: str):
    """Pick a deterministic path through the ORG tree under the chosen division."""
    units = list(ORG[division].keys())
    unit  = stable_choice(seed, 'unit', units)
    areas = list(ORG[division][unit].keys())
    area  = stable_choice(seed, 'area', areas)
    sectors = list(ORG[division][unit][area].keys())
    sector  = stable_choice(seed, 'sector', sectors)
    segments_node = ORG[division][unit][area][sector]
    if isinstance(segments_node, list):
        segment  = stable_choice(seed, 'segment', segments_node)
        function = stable_choice(seed, 'function', segments_node)  # leaf: same pool
    else:
        segments = list(segments_node.keys())
        segment  = stable_choice(seed, 'segment', segments)
        functions = segments_node[segment]
        if isinstance(functions, list):
            function = stable_choice(seed, 'function', functions)
        else:
            funcs = list(functions.keys())
            function = stable_choice(seed, 'function', funcs)
    return unit, area, sector, segment, function


def make_ou(seed: str, division: str, unit: str, area: str) -> tuple[str, str]:
    """Compact OU description + 4-digit code."""
    h = hashlib.sha256(f'{seed}|ou'.encode()).hexdigest()
    code = str(int(h[:8], 16) % 9000 + 1000)
    short = ''.join(w[0] for w in division.split() if w)[:3].upper()
    return f'{short}-{area[:14]}', code


def main():
    if not EVENTS_PATH.exists():
        raise SystemExit(f'events_raw.parquet not found at {EVENTS_PATH}. Run process_clicks.py first.')

    con = duckdb.connect()
    gpns = [r[0] for r in con.execute(
        f"SELECT DISTINCT gpn FROM read_parquet('{EVENTS_PATH}') WHERE gpn IS NOT NULL ORDER BY 1"
    ).fetchall()]
    print(f'Loaded {len(gpns)} distinct GPNs from events_raw.parquet')

    snapshot_year, snapshot_month = 2026, 1
    rows = []
    for gpn in gpns:
        gpn_str = str(gpn).zfill(8)
        seed = gpn_str

        division = stable_choice(seed, 'division', DIVISION_WEIGHTS)
        unit, area, sector, segment, function = walk_org(seed, division)
        ou_desc, ou_code = make_ou(seed, division, unit, area)

        region  = stable_choice(seed, 'region',  REGION_WEIGHTS_BY_DIVISION[division])
        country = stable_choice(seed, 'country', REGION_COUNTRIES[region])

        rows.append({
            'gpn': gpn_str,
            'snapshot_year':  snapshot_year,
            'snapshot_month': snapshot_month,
            'gcrs_division_desc': division,
            'gcrs_unit_desc':     unit,
            'gcrs_area_desc':     area,
            'gcrs_sector_desc':   sector,
            'gcrs_segment_desc':  segment,
            'gcrs_function_desc': function,
            'ou_desc': ou_desc,
            'ou_code': ou_code,
            'work_location_country': country,
            'work_location_region':  region,
        })

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f'Wrote {len(df):,} rows to {OUT_PATH}')
    print('\nDivision distribution:')
    print(df['gcrs_division_desc'].value_counts().to_string())
    print('\nRegion distribution:')
    print(df['work_location_region'].value_counts().to_string())


if __name__ == '__main__':
    main()
