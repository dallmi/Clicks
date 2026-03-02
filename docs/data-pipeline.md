# Clicks Data Pipeline

## Overview

This pipeline processes click data extracted from Azure Application Insights across **all intranet pages**. It ingests KQL exports (`.xlsx` or `.csv`), enriches them with HR organisational data, computes derived metrics, and exports Parquet files for downstream analysis.

Unlike the CampaignWe pipeline (which focuses on a single page), this project captures every `click_event` without page filtering or action type classification.

> **Terminology**: Every record represents a **click** — a user clicking a link, button, or element on any intranet page. The source Application Insights event type is `click_event`. Database tables use the name `events` (inherited from App Insights conventions).

```
Application Insights (KQL)
        |
        v
  input/*.xlsx / *.csv        <-- you drop files here
        |
        v
  process_clicks.py            <-- delta detection + upsert + enrichment
        |
        +---> data/clicks.db                  (DuckDB database)
        +---> output/events_raw.parquet       (all events with HR fields)
        +---> output/events_anonymized.parquet (anonymized: GPNs hashed, emails dropped)
```

---

## Input Files

### Source

Export click events from Application Insights using the KQL query in `clicks_query.kql`. The query targets `customEvents` where `name == "click_event"` — no page filter is applied.

### File Naming Convention

```
clicks_export_YYYY_MM_DD.xlsx
clicks_export_YYYY_MM_DD.csv
```

The `_YYYY_MM_DD` date suffix is mandatory for correct ordering. Files without a date suffix fall back to filesystem modification time.

### Placement

Drop files into the `input/` folder. The script scans this folder automatically.

### Format Recommendations

| Format | Timestamp Precision | GPN Handling | Recommendation |
|--------|-------------------|--------------|----------------|
| CSV | Microsecond (full) | String (safe) | Preferred |
| XLSX | Second only (truncated) | May lose leading zeros | Use if CSV unavailable |

CSV preserves the full `dd/MM/yyyy HH:mm:ss.fffffff` timestamp from App Insights. Excel truncates to whole seconds, which weakens the composite primary key's uniqueness.

### Expected Columns

| Column | Source | Notes |
|--------|--------|-------|
| `timestamp [UTC]` | App Insights | Renamed to `timestamp` during load |
| `name` | App Insights | Event type, typically `click_event` |
| `user_Id` | App Insights | Renamed to `user_id` |
| `session_Id` | App Insights | Renamed to `session_id` |
| `client_CountryOrRegion` | App Insights | Geographic info |
| `CP_GPN` / `CP_gpn` | CustomProps | Global Personnel Number (8 digits) |
| `CP_Email` | CustomProps | User email |
| `CP_Link_Type` | CustomProps | Type of link clicked |
| `CP_Link_label` | CustomProps | Label text of clicked element |
| `CP_ComponentName` | CustomProps | Component that generated the click |
| `CP_SiteName` | CustomProps | Intranet site name |
| `CP_PageURL` | CustomProps | Page where click occurred |
| `CP_ContentType` | CustomProps | Content type of the page |
| `CP_FileType_Label` | CustomProps | File type (for downloads) |
| `CP_FileName_Label` | CustomProps | File name (for downloads) |
| `CP_Link_address` | CustomProps | Target URL of the click |

---

## Running the Script

### Prerequisites

```bash
pip install duckdb pandas openpyxl
```

### Usage

```bash
# Delta mode (default) -- process only new or changed files
python process_clicks.py

# Force-process a specific file (bypasses delta check)
python process_clicks.py input/clicks_export_2026_02_25.xlsx

# Full refresh -- delete database and reprocess all files from scratch
python process_clicks.py --full-refresh
```

### Typical Workflow

1. Export data from App Insights (daily or weekly)
2. Save the `.xlsx` or `.csv` file to `input/`
3. Run `python process_clicks.py`

---

## Delta Processing

The script tracks which files have already been processed using a `processed_files` manifest table inside the DuckDB database.

### How It Works

On each run (without `--full-refresh` or a specific file argument):

1. **Scan** `input/` for all `.xlsx`, `.xls`, `.csv` files
2. **Hash** each file's contents (SHA-256)
3. **Compare** against the `processed_files` table in the database:
   - **New filename** — file is processed
   - **Same filename, same hash** — file is skipped (already processed)
   - **Same filename, different hash** — file is re-processed (contents changed)
4. **Record** successfully processed files in the manifest

### Manifest Table Schema

```sql
CREATE TABLE processed_files (
    filename     TEXT PRIMARY KEY,
    file_hash    TEXT,
    row_count    INTEGER,
    processed_at TIMESTAMP,
    date_suffix  DATE
);
```

### Behaviour by Scenario

| Scenario | What Happens |
|----------|-------------|
| First run, 3 files in `input/` | All 3 processed oldest-first, all recorded in manifest |
| Second run, no new files | "All files already processed. Nothing new to do." |
| New file added to `input/` | Only the new file is processed |
| File replaced (same name, new content) | Hash mismatch detected, file re-processed |
| `--full-refresh` | Database deleted (including manifest), all files reprocessed |
| Explicit file argument | File is force-processed regardless of manifest state |

---

## Upsert Logic (Overlap Handling)

Weekly or daily exports from App Insights may contain overlapping date ranges. The script uses a **delete-then-insert** upsert pattern to prevent double-counting.

### Primary Key

```
(timestamp, user_id, session_id, name)
```

### Mechanism

```sql
-- Step 1: Delete existing rows that match incoming rows on the composite key
DELETE FROM events_raw
WHERE EXISTS (
    SELECT 1 FROM temp_import t
    WHERE events_raw.timestamp = t.timestamp
      AND events_raw.user_id = t.user_id
      AND events_raw.session_id = t.session_id
      AND events_raw.name = t.name
);

-- Step 2: Insert all rows from the new file
INSERT INTO events_raw SELECT * FROM temp_import;
```

### Precision Warning

The composite key relies on timestamp uniqueness. If two identical events from the same user/session occur in the same second (possible when Excel truncates microseconds), they will be treated as one event. **Export as CSV to preserve microsecond precision.**

---

## Processing Pipeline

After file loading and upsert, the script runs these stages:

### 1. HR History Join

Loads `hr_history.parquet` from `../SearchAnalytics/output/` and joins on GPN with time-aware matching:

- **Primary match**: Most recent HR snapshot where `snapshot_date <= event_date`
- **Fallback match**: Closest following snapshot (for events before the first snapshot)

This adds organisational fields: `hr_division`, `hr_unit`, `hr_area`, `hr_sector`, `hr_segment`, `hr_function`, `hr_country`, `hr_region`, etc.

### 2. Calculated Columns

| Column | Description |
|--------|-------------|
| `gpn` | Normalised 8-digit GPN (zero-padded, `.0` stripped) |
| `email` | Resolved from available email columns |
| `timestamp_cet` | UTC converted to Europe/Berlin timezone |
| `session_date` | CET-based date (for daily bucketing) |
| `session_key` | `YYYY-MM-DD_user_id_session_id` (unique session identifier) |
| `event_hour` | Hour in CET (0-23) |
| `event_weekday` | Day name (Monday, Tuesday, ...) |
| `event_order` | Sequence number within session |
| `prev_event` / `prev_timestamp` | Previous event in session (for flow analysis) |
| `ms_since_prev_event` | Milliseconds since previous event |
| `time_since_prev_bucket` | Categorised interval (< 0.5s, 0.5-1s, 1-2s, ..., > 60s) |

### 3. Parquet Export

| File | Contents | Grain |
|------|----------|-------|
| `events_raw.parquet` | All events with all calculated + HR columns | One row per event |
| `events_anonymized.parquet` | Same as above but GPNs hashed, emails dropped | One row per event |

---

## Output Database

The DuckDB database at `data/clicks.db` contains:

| Table | Description |
|-------|-------------|
| `events_raw` | Raw imported data (pre-enrichment) |
| `events` | Final enriched table with all calculated columns |
| `hr_history` | HR organisational data (loaded each run) |
| `processed_files` | File processing manifest for delta tracking |

---

## Exploratory Analysis

The file `clicks_explorer.kql` contains 13 independently runnable KQL queries for understanding the click landscape before building dashboards or reports. Topics covered:

- Volume overview and date range
- Event name discovery (beyond `click_event`)
- CustomProps schema discovery
- Link Type and Link Label distributions
- Component and Site breakdowns
- Page URL patterns
- Content Type analysis
- Internal vs external link targets
- File type downloads
- Daily volume trends

Run these queries directly in the App Insights query editor to explore the data before exporting.

---

## Troubleshooting

### "All files already processed"

The manifest shows all files have matching hashes. Either:
- Drop a new file into `input/` and re-run
- Use `--full-refresh` to reprocess everything
- Pass a specific file path to force-process it

### Timestamp precision warning

```
WARNING: Column 'timestamp' has no microsecond precision!
```

This means the input file (likely `.xlsx`) has truncated timestamps. Export from App Insights as CSV instead.

### HR history not found

```
WARNING: HR history file not found
```

The script expects `../SearchAnalytics/output/hr_history.parquet`. Run the SearchAnalytics HR processing script first, or the pipeline will proceed without HR enrichment.
