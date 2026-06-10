# Standalone Click-Tracking Dashboard (SharePoint distribution)

A single self-contained HTML build of `dashboard2.html` that runs from a downloaded
file — **no Python server required**. For sharing via SharePoint without IT / app-catalog
involvement.

The build inlines the events parquet as base64 (column-pruned + ZSTD) and inlines Chart.js
and ExcelJS from local vendored copies. DuckDB-WASM stays a runtime CDN load from jsdelivr
(which works from `file://`).

## The two modes — one template

`dashboard/dashboard2.html` runs in both modes via a loader shim:

- **Dev mode** (served over http, e.g. `python3 -m http.server`): `window.__EVENTS_B64__`
  is `null`, so it `fetch()`es `output/events_raw.parquet`.
- **Standalone mode** (`file://`): the build replaces that placeholder with the base64 data,
  which DuckDB-WASM decodes in-memory. No server, no fetch.

## Rebuild after a data update

1. Regenerate `output/events_raw.parquet` by running the pipeline (`scripts/process_clicks.py`).
2. Run the build:

```bash
python3 scripts/build_standalone.py
```

Defaults: `--events output/events_raw.parquet`, `--template dashboard/dashboard2.html`,
`--months 0` (keep all data), output `output/click_dashboard_standalone.html`. The build
prints the row count, embedded parquet size and final file size, and warns above 8 MB.

To window the data (smaller file), pass `--months 6`. To build from other data, pass
`--events <path>`.

## Refreshing the vendored libraries (rare)

The JS libraries are read from local copies in `dashboard/vendor/` (committed). To refresh
on a version bump, re-download from jsdelivr:

```bash
curl -sS -o dashboard/vendor/exceljs.min.js   https://cdn.jsdelivr.net/npm/exceljs@4.4.0/dist/exceljs.min.js
curl -sS -o dashboard/vendor/chart.umd.min.js https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js
```

(Keep the versions in sync with the CDN `<script>` tags in `dashboard2.html`.)

## How users open it

Clicking the file in SharePoint **downloads** it (modern SharePoint force-downloads HTML).
Users open the downloaded file by double-clicking it. It runs from `file://` — the data is
embedded. Internet access to `cdn.jsdelivr.net` is required at open time for DuckDB-WASM.

## Notes / limits

- Data is a **snapshot** baked in at build time — rebuild + re-upload to refresh.
- The file embeds the events data; treat it with the same classification as the source
  parquet. `output/` (incl. the standalone HTML) is gitignored so data is never pushed to git.
- Needs internet on open only for DuckDB-WASM (jsdelivr). Everything else — dashboard code,
  charts, XLSX export, the data — is in the file.
