"""Build a self-contained standalone HTML from the click-tracking dashboard.

Bakes the (column-pruned, ZSTD-recompressed) events parquet in as base64 and
inlines the vendored JS libraries so the dashboard runs from file:// with no
server — for sharing via SharePoint without IT / app-catalog involvement.

DuckDB-WASM stays a runtime CDN load from jsdelivr (works from file://), exactly
like the Brightcove standalone build.
"""
import argparse
import base64
import re
import tempfile
from pathlib import Path

import duckdb

# Columns the dashboard's queries actually read (the contract). Pruning to these
# shrinks the baked-in parquet ~5x. Resolved against the source — any column the
# source happens not to have is silently skipped, so this stays robust across
# pipeline changes.
KEEP_COLS = [
    "name", "session_date", "timestamp_cet",
    "CP_SiteName", "CP_SiteID", "CP_PageName", "CP_PageURL",
    "CP_Link_Type", "CP_Link_label", "CP_Link_address",
    "CP_FileType_Label", "CP_FileName_Label",
    "CP_Topic", "CP_Theme", "CP_TargetOrg", "CP_TargetRegion",
    "CP_GPN", "gpn",
    "hr_division", "hr_unit", "hr_area", "hr_sector",
    "hr_segment", "hr_function", "hr_region", "hr_country",
]

# CDN-src substring (in the template) -> vendored filename in dashboard/vendor/.
VENDOR_MAP = {
    "exceljs@4.4.0/dist/exceljs.min.js": "exceljs.min.js",
    "chart.js@4.4.1/dist/chart.umd.min.js": "chart.umd.min.js",
}

WARN_MB = 8.0
ERROR_MB = 25.0


def _columns(con, rel):
    return [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()]


def parquet_to_b64(con, src_path, months, tmp_dir):
    """COPY a pruned/windowed subset of the events parquet to ZSTD parquet.

    Returns (base64_str, parquet_bytes, rowcount). `months` keeps only the last
    N months relative to the max session_date (0 = keep everything).
    """
    src = Path(src_path).as_posix()
    rel = f"read_parquet('{src}')"
    cols = _columns(con, rel)
    keep = [c for c in KEEP_COLS if c in cols]
    if "session_date" not in keep:
        raise ValueError("source parquet has no session_date column")

    where = ""
    if months and months > 0:
        where = (
            f"WHERE session_date >= "
            f"(SELECT max(session_date) FROM {rel}) - INTERVAL '{int(months)} months'"
        )

    out = Path(tmp_dir) / "events.parquet"
    con.execute(
        f"COPY (SELECT {','.join(keep)} FROM {rel} {where}) "
        f"TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    raw = out.read_bytes()
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
    return base64.b64encode(raw).decode("ascii"), len(raw), rows


def inject_data(html, events_b64):
    """Replace `window.__EVENTS_B64__=null` with the encoded data."""
    needle = "window.__EVENTS_B64__=null"
    if needle not in html:
        raise ValueError(f"placeholder not found in template: {needle}")
    return html.replace(needle, f'window.__EVENTS_B64__="{events_b64}"', 1)


def inline_vendored(html, vendor_dir):
    """Replace each CDN <script src=...></script> with an inline vendored copy."""
    vendor_dir = Path(vendor_dir)
    for substr, fname in VENDOR_MAP.items():
        content = (vendor_dir / fname).read_text(encoding="utf-8")
        pattern = re.compile(
            r'<script[^>]*\ssrc="[^"]*' + re.escape(substr) + r'"[^>]*></script>'
        )
        if not pattern.search(html):
            raise ValueError(f"vendor script tag not found in template: {substr}")
        # Function replacement so backslashes in `content` are not treated as
        # regex group references.
        html = pattern.sub(
            lambda m, c=content, f=fname: f"<script>\n/* vendored {f} */\n{c}\n</script>",
            html,
            count=1,
        )
    return html


def build(src_path, template_path, vendor_dir, months, output_path):
    """Produce the standalone HTML; return a stats dict. Raises on guard violations."""
    con = duckdb.connect(":memory:")
    try:
        with tempfile.TemporaryDirectory() as td:
            events_b64, parquet_bytes, rows = parquet_to_b64(con, src_path, months, td)
    finally:
        con.close()

    if rows == 0:
        raise ValueError(f"no rows within the last {months} months")

    html = Path(template_path).read_text(encoding="utf-8")
    html = inject_data(html, events_b64)
    html = inline_vendored(html, vendor_dir)

    output_path = Path(output_path)
    output_path.write_text(html, encoding="utf-8")
    size_mb = output_path.stat().st_size / (1024 * 1024)

    if size_mb > ERROR_MB:
        output_path.unlink(missing_ok=True)
        raise ValueError(
            f"output is {size_mb:.1f} MB (> {ERROR_MB} MB). Reduce --months and rebuild."
        )

    return {
        "rows": rows,
        "parquet_bytes": parquet_bytes,
        "size_mb": size_mb,
        "warn": size_mb > WARN_MB,
    }


def main(argv=None):
    here = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description="Build the standalone SharePoint click dashboard.")
    ap.add_argument("--events", type=Path, default=here / "output" / "events_raw.parquet",
                    help="path to events_raw.parquet (defaults to pipeline output)")
    ap.add_argument("--template", type=Path,
                    default=here / "dashboard" / "dashboard.html")
    ap.add_argument("--vendor", type=Path, default=here / "dashboard" / "vendor")
    ap.add_argument("--months", type=int, default=0,
                    help="keep only the last N months of data (0 = all)")
    ap.add_argument("--output", type=Path,
                    default=here / "output" / "click_dashboard_standalone.html")
    args = ap.parse_args(argv)

    if not args.events.exists():
        ap.error(f"--events file not found: {args.events}\n"
                 f"Run the pipeline (process_clicks.py) first, or pass an explicit path.")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    stats = build(
        src_path=args.events,
        template_path=args.template,
        vendor_dir=args.vendor,
        months=args.months,
        output_path=args.output,
    )
    print(f"rows    : {stats['rows']:,}")
    print(f"parquet : {stats['parquet_bytes'] / 1e6:.2f} MB (ZSTD, pruned)")
    print(f"output  : {args.output}  ({stats['size_mb']:.1f} MB)")
    if stats["warn"]:
        print(f"WARNING: output exceeds {WARN_MB} MB; consider --months to window the data.")


if __name__ == "__main__":
    main()
