#!/usr/bin/env python3
"""
Intranet Clicks Analytics Processing Script

This script processes click_event data extracted via KQL from Application Insights
for all intranet pages. It creates/updates a DuckDB database, joins with
HR data from hr_history.parquet via GPN, and exports Parquet files for reporting.

Unlike the CampaignWe pipeline, this does NOT filter by page URL and does NOT
classify clicks into action types. All click events are retained as-is.

Usage:
    python process_clicks.py                     # Process only new/changed files (delta)
    python process_clicks.py input/export.xlsx   # Force-process a specific file
    python process_clicks.py --full-refresh      # Delete DB and reprocess all files

Input folder: input/
    Place your KQL export files here with date suffix _YYYY_MM_DD, e.g.:
    - clicks_export_2026_02_25.xlsx
    - clicks_export_2026_02_25.csv

    Only new or modified files are processed (tracked via SHA-256 hash).
    Overlapping time ranges are handled via upsert on the primary key.

Output:
    - data/clicks.db                     (DuckDB database)
    - output/events_raw.parquet          (all event-level data with HR fields)
    - output/events_anonymized.parquet   (anonymized: GPNs hashed, emails dropped)
    - output/cdm/dim_date.parquet        (CDM: date dimension, shared across pipelines)
    - output/cdm/dim_organization.parquet(CDM: HR org dimension, shared across pipelines)
    - output/cdm/dim_site.parquet        (CDM: site dimension)
    - output/cdm/dim_page.parquet        (CDM: page dimension)
    - output/cdm/dim_link_type.parquet   (CDM: link type dimension)
    - output/cdm/dim_component.parquet   (CDM: component dimension)
    - output/cdm/fact_clicks.parquet     (CDM: fact table with FK integers, anonymized)

Primary Key: timestamp + user_id + session_id + name
    On conflict, the latest file's data takes precedence.
"""

import sys
import os
import re
import glob
import hashlib
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    """
    Extract date from filename with format _YYYY_MM_DD.
    Returns a date object or None if not found.
    """
    filename = Path(filepath).stem
    match = re.search(r'_(\d{4})_(\d{2})_(\d{2})$', filename)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day).date()
        except ValueError:
            return None
    return None


def find_latest_input_file(input_dir):
    """
    Find the latest input file in the input directory based on date in filename.
    Expects format: filename_YYYY_MM_DD.xlsx or filename_YYYY_MM_DD.csv
    """
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    if not all_files:
        return None

    files_with_dates = []
    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))

    if not files_with_dates:
        log("  Warning: No files with _YYYY_MM_DD suffix found, using modification time")
        all_files.sort(key=os.path.getmtime, reverse=True)
        return Path(all_files[0])

    files_with_dates.sort(key=lambda x: x[1], reverse=True)
    return files_with_dates[0][0]


def get_all_input_files(input_dir):
    """Get all input files sorted by date in filename (oldest first for processing order)."""
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    files_with_dates = []
    files_without_dates = []

    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))
        else:
            files_without_dates.append(Path(f))

    files_with_dates.sort(key=lambda x: x[1])

    result = [f for f, _ in files_with_dates]
    files_without_dates.sort(key=os.path.getmtime)
    result.extend(files_without_dates)

    return result


def compute_file_hash(filepath):
    """SHA-256 hash of file contents for change detection."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def ensure_manifest_table(con):
    """Create processed_files manifest table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename     TEXT PRIMARY KEY,
            file_hash    TEXT,
            row_count    INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_suffix  DATE
        )
    """)


def get_unprocessed_files(con, input_dir):
    """
    Return list of (filepath, hash, reason) for files that are new or changed.
    Compares SHA-256 hashes against the processed_files manifest in DuckDB.
    """
    ensure_manifest_table(con)
    all_files = get_all_input_files(input_dir)

    to_process = []
    skipped = []

    for filepath in all_files:
        file_hash = compute_file_hash(filepath)
        filename = filepath.name

        existing = con.execute(
            "SELECT file_hash FROM processed_files WHERE filename = ?",
            [filename]
        ).fetchone()

        if existing is None:
            to_process.append((filepath, file_hash, 'new'))
        elif existing[0] != file_hash:
            to_process.append((filepath, file_hash, 'changed'))
        else:
            skipped.append(filename)

    if skipped:
        log(f"  Skipping {len(skipped)} already-processed file(s): {', '.join(skipped)}")
    if to_process:
        log(f"  Found {len(to_process)} file(s) to process")

    return to_process


def record_processed_file(con, filepath, file_hash, row_count):
    """Record a successfully processed file in the manifest."""
    filename = filepath.name
    date_suffix = extract_date_from_filename(filepath)
    # Use INSERT OR REPLACE to update existing entries (e.g. changed files)
    con.execute("""
        DELETE FROM processed_files WHERE filename = ?
    """, [filename])
    con.execute("""
        INSERT INTO processed_files (filename, file_hash, row_count, processed_at, date_suffix)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
    """, [filename, file_hash, int(row_count), date_suffix])


def load_file_to_temp_table(con, input_path, temp_table='temp_import'):
    """Load a CSV or Excel file into a temporary table."""
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        # First pass: read only column names
        df_cols = pd.read_excel(input_path, nrows=0)
        all_cols = df_cols.columns.tolist()
        timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]
        # GPN columns must be read as string to preserve leading zeros
        gpn_cols = [col for col in all_cols if col.lower() in ('cp_gpn', 'gpn')]

        # Read Excel with specific columns forced to string type
        dtype_dict = {}
        if timestamp_cols:
            dtype_dict.update({col: str for col in timestamp_cols})
            log(f"  Reading timestamp columns as strings: {timestamp_cols}")
        if gpn_cols:
            dtype_dict.update({col: str for col in gpn_cols})
            log(f"  Reading GPN columns as strings: {gpn_cols}")

        if dtype_dict:
            df = pd.read_excel(input_path, dtype=dtype_dict)
        else:
            df = pd.read_excel(input_path)

        con.register('excel_df', df)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM excel_df")
        con.unregister('excel_df')
    else:
        con.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM read_csv('{input_path}', auto_detect=true)
        """)

    # Normalize column names
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    col_names = schema['column_name'].tolist()

    rename_map = {
        'user_Id': 'user_id',
        'session_Id': 'session_id',
        'timestamp [UTC]': 'timestamp'
    }
    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{old_name}" TO {new_name}')

    # Convert date formats (German dd.MM.yyyy and App Insights dd/MM/yyyy)
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = con.execute(f'SELECT "{col}" FROM {temp_table} WHERE "{col}" IS NOT NULL LIMIT 1').df()
        if len(sample) > 0:
            val = str(sample.iloc[0, 0])
            fmt = None

            if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%d/%m/%Y %H:%M:%S.%f'
                frac_part = val.split('.')[-1]
                if len(frac_part) > 6:
                    fmt = 'TRUNCATE_FRAC'
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M:%S'
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M'
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', val):
                fmt = '%d/%m/%Y'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M:%S'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4}$', val):
                fmt = '%d.%m.%Y'
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%Y-%m-%d %H:%M:%S.%f'
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%Y-%m-%d %H:%M:%S'
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', val):
                fmt = 'ISO'

            if fmt == 'TRUNCATE_FRAC':
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'''
                        UPDATE {temp_table} SET "{col}_temp" = strptime(
                            CASE
                                WHEN "{col}" LIKE '%.%'
                                THEN SUBSTRING("{col}", 1, POSITION('.' IN "{col}") + 6)
                                ELSE "{col}"
                            END,
                            '%d/%m/%Y %H:%M:%S.%f'
                        )
                    ''')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception as e:
                    log(f"  WARNING: Failed to convert '{col}' with truncation: {e}")
            elif fmt == 'ISO':
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = CAST("{col}" AS TIMESTAMP)')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass
            elif fmt:
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = strptime("{col}", \'{fmt}\')')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass

    # Fallback: Try to convert any remaining VARCHAR timestamp column using CAST
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    for _, row in schema.iterrows():
        col = row['column_name']
        col_type = row['column_type']
        if col.lower() == 'timestamp' and col_type == 'VARCHAR':
            try:
                con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                con.execute(f'UPDATE {temp_table} SET "{col}_temp" = TRY_CAST("{col}" AS TIMESTAMP)')
                con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                log(f"  Converted '{col}' to TIMESTAMP using TRY_CAST")
            except Exception as e:
                log(f"  WARNING: Could not convert '{col}' to TIMESTAMP: {e}")

    # Check for timestamp precision
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    timestamp_cols = [col for col in schema['column_name'].tolist()
                      if 'timestamp' in col.lower()]

    for col in timestamp_cols:
        try:
            result = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM {temp_table}
                WHERE EXTRACT(microsecond FROM "{col}") != 0
            """).df()
            has_microseconds = result['cnt'][0] > 0

            if not has_microseconds:
                log(f"  WARNING: Column '{col}' has no microsecond precision!")
                log(f"           For precise timing, export from App Insights as CSV (not Excel).")
        except Exception:
            pass

    row_count = con.execute(f"SELECT COUNT(*) as n FROM {temp_table}").df()['n'][0]
    return row_count


def upsert_data(con, temp_table='temp_import'):
    """
    Upsert data from temp table into main events_raw table.
    Primary key: timestamp + user_id + session_id + name
    """
    tables = con.execute("SHOW TABLES").df()
    table_exists = 'events_raw' in tables['name'].values if len(tables) > 0 else False

    if not table_exists:
        con.execute(f"ALTER TABLE {temp_table} RENAME TO events_raw")
        log("  Created new events_raw table")
        return

    before_count = con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]

    con.execute(f"""
        DELETE FROM events_raw
        WHERE EXISTS (
            SELECT 1 FROM {temp_table} t
            WHERE events_raw.timestamp = t.timestamp
              AND events_raw.user_id = t.user_id
              AND events_raw.session_id = t.session_id
              AND events_raw.name = t.name
        )
    """)

    deleted_count = before_count - con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]

    con.execute(f"""
        INSERT INTO events_raw
        SELECT * FROM {temp_table}
    """)

    after_count = con.execute("SELECT COUNT(*) as n FROM events_raw").df()['n'][0]
    new_rows = after_count - before_count + deleted_count

    if deleted_count > 0:
        log(f"  Updated {deleted_count:,} existing rows, added {new_rows - deleted_count:,} new rows")
    else:
        log(f"  Added {new_rows:,} new rows")

    con.execute(f"DROP TABLE IF EXISTS {temp_table}")


def load_hr_history(con, hr_parquet_path):
    """
    Load hr_history.parquet into DuckDB for GPN-based joining.
    Returns True if loaded successfully, False otherwise.
    """
    if not hr_parquet_path.exists():
        log(f"  WARNING: HR history file not found: {hr_parquet_path}")
        log(f"           Run process_hr_history.py in SearchAnalytics first.")
        return False

    con.execute("DROP TABLE IF EXISTS hr_history")
    con.execute(f"""
        CREATE TABLE hr_history AS
        SELECT * FROM read_parquet('{hr_parquet_path}')
    """)

    row_count = con.execute("SELECT COUNT(*) FROM hr_history").fetchone()[0]
    gpn_count = con.execute("SELECT COUNT(DISTINCT gpn) FROM hr_history").fetchone()[0]
    snapshot_count = con.execute(
        "SELECT COUNT(DISTINCT (snapshot_year, snapshot_month)) FROM hr_history"
    ).fetchone()[0]

    log(f"  Loaded hr_history: {row_count:,} rows, {gpn_count:,} GPNs, {snapshot_count} snapshot(s)")
    return True


def add_calculated_columns(con, has_hr_history=False):
    """Add all calculated columns to events_raw and create final events table."""
    log("Adding calculated columns...")

    con.execute("DROP TABLE IF EXISTS events")

    # Set timezone to UTC so DuckDB interprets naive timestamps as UTC
    con.execute("SET TIMEZONE='UTC'")

    # Get column list
    schema = con.execute("DESCRIBE events_raw").df()
    col_names = schema['column_name'].tolist()

    has_user_id = 'user_id' in col_names
    has_session_id = 'session_id' in col_names
    has_timestamp = 'timestamp' in col_names

    # --- Dynamic column resolution ---
    # GPN field (for HR join)
    gpn_candidates = [c for c in ['CP_GPN', 'CP_gpn', 'GPN', 'gpn'] if c in col_names]
    if gpn_candidates:
        gpn_expr = f"LPAD(REGEXP_REPLACE(CAST(COALESCE({', '.join(gpn_candidates)}) AS VARCHAR), '\\.0$', ''), 8, '0')"
    else:
        gpn_expr = 'NULL'

    # Email field
    email_candidates = [c for c in ['Email', 'email', 'CP_Email', 'CP_email'] if c in col_names]
    email_expr = f"COALESCE({', '.join(email_candidates)})" if email_candidates else 'NULL'

    # Log resolution
    log(f"  GPN column resolved from: [{', '.join(gpn_candidates) if gpn_candidates else 'none found'}]")
    log(f"  Email column resolved from: [{', '.join(email_candidates) if email_candidates else 'none found'}]")

    # HR join expression
    if has_hr_history and gpn_candidates:
        # Discover available HR columns
        hr_schema = con.execute("DESCRIBE hr_history").df()
        hr_cols = hr_schema['column_name'].tolist()

        hr_field_map = {
            'gcrs_division_desc': 'hr_division',
            'gcrs_unit_desc': 'hr_unit',
            'gcrs_area_desc': 'hr_area',
            'gcrs_sector_desc': 'hr_sector',
            'gcrs_segment_desc': 'hr_segment',
            'gcrs_function_desc': 'hr_function',
            'ou_code': 'hr_ou_code',
            'work_location_country': 'hr_country',
            'work_location_region': 'hr_region',
            'job_title': 'hr_job_title',
            'job_family': 'hr_job_family',
            'management_level': 'hr_management_level',
            'cost_center': 'hr_cost_center',
        }

        available_hr_fields = {src: alias for src, alias in hr_field_map.items() if src in hr_cols}
        log(f"  HR fields available: {list(available_hr_fields.keys())}")

        hr_select_parts = [f'h.{src} as {alias}' for src, alias in available_hr_fields.items()]
        hr_select_sql = ', '.join(hr_select_parts) if hr_select_parts else 'NULL as hr_placeholder'

        # Diagnostic: show sample GPNs from both sides
        try:
            event_gpn_sample = con.execute(f"""
                SELECT DISTINCT {gpn_expr} as gpn FROM events_raw
                WHERE {gpn_expr} IS NOT NULL AND TRIM({gpn_expr}) != ''
                LIMIT 5
            """).df()
            hr_gpn_sample = con.execute("""
                SELECT DISTINCT CAST(gpn AS VARCHAR) as gpn FROM hr_history
                LIMIT 5
            """).df()
            log(f"  Sample event GPNs: {event_gpn_sample['gpn'].tolist()}")
            log(f"  Sample HR GPNs:    {hr_gpn_sample['gpn'].tolist()}")
        except Exception:
            pass

        hr_join_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month) <= (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year DESC, h.snapshot_month DESC
                LIMIT 1
            ) hr_exact ON true
        """

        hr_fallback_sql = f"""
            LEFT JOIN LATERAL (
                SELECT {hr_select_sql}
                FROM hr_history h
                WHERE CAST(h.gpn AS VARCHAR) = {gpn_expr}
                  AND (h.snapshot_year * 100 + h.snapshot_month) > (YEAR(r.timestamp) * 100 + MONTH(r.timestamp))
                ORDER BY h.snapshot_year ASC, h.snapshot_month ASC
                LIMIT 1
            ) hr_fallback ON true
        """

        hr_coalesce_parts = []
        for src, alias in available_hr_fields.items():
            hr_coalesce_parts.append(f"COALESCE(hr_exact.{alias}, hr_fallback.{alias}) as {alias}")
        hr_coalesce_sql = ', '.join(hr_coalesce_parts) if hr_coalesce_parts else ''
    else:
        hr_join_sql = ''
        hr_fallback_sql = ''
        hr_coalesce_sql = ''
        available_hr_fields = {}

    # Build the main query with all calculated columns
    hr_select = f",\n            {hr_coalesce_sql}" if hr_coalesce_sql else ''

    con.execute(f"""
        CREATE TABLE events AS
        SELECT
            r.*,
            -- GPN and email extracted for reference
            {gpn_expr} as gpn,
            {email_expr} as email,
            -- Timestamp as string for reporting (UTC)
            STRFTIME(r.timestamp, '%Y-%m-%d %H:%M:%S.%g') as timestamp_str,
            -- CET timestamp (convert UTC to Europe/Berlin)
            ((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::TIMESTAMP as timestamp_cet,
            STRFTIME((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin', '%Y-%m-%d %H:%M:%S.%g') as timestamp_cet_str,
            -- Session columns (CET-based)
            DATE_TRUNC('day', (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE as session_date,
            COALESCE(CAST(DATE_TRUNC('day', (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE AS VARCHAR), '') || '_' ||
                COALESCE(r.user_id, '') || '_' ||
                COALESCE(r.session_id, '') as session_key,
            -- Time extraction (CET-based)
            EXTRACT(HOUR FROM (r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::INTEGER as event_hour,
            DAYNAME((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday,
            ISODOW((r.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday_num,
            -- Event ordering (populated via window functions in next step)
            NULL::INTEGER as event_order,
            NULL::VARCHAR as prev_event,
            NULL::TIMESTAMP as prev_timestamp,
            NULL::BIGINT as ms_since_prev_event,
            NULL::DOUBLE as sec_since_prev_event,
            NULL::VARCHAR as time_since_prev_bucket
            {hr_select}
        FROM events_raw r
        {hr_join_sql}
        {hr_fallback_sql}
    """)

    # Now update the window function columns
    con.execute("""
        CREATE OR REPLACE TABLE events AS
        SELECT
            e.* EXCLUDE (event_order, prev_event, prev_timestamp, ms_since_prev_event, sec_since_prev_event, time_since_prev_bucket),
            ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY timestamp) as event_order,
            LAG(name) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_event,
            LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_timestamp,
            DATEDIFF('millisecond',
                LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                timestamp
            ) as ms_since_prev_event,
            ROUND(
                DATEDIFF('millisecond',
                    LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                    timestamp
                ) / 1000.0,
            3) as sec_since_prev_event,
            CASE
                WHEN LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) IS NULL THEN 'First Event'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 500 THEN '< 0.5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 1000 THEN '0.5-1s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 2000 THEN '1-2s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 5000 THEN '2-5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 10000 THEN '5-10s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 30000 THEN '10-30s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 60000 THEN '30-60s'
                ELSE '> 60s'
            END as time_since_prev_bucket
        FROM events e
    """)

    row_count = con.execute("SELECT COUNT(*) as n FROM events").df()['n'][0]
    log(f"  Calculated columns added for {row_count:,} rows")

    # Verify CET timezone conversion
    cet_sample = con.execute("""
        SELECT
            timestamp as utc_timestamp,
            timestamp_cet as cet_timestamp,
            EXTRACT(HOUR FROM timestamp) as utc_hour,
            event_hour as cet_hour,
            session_date
        FROM events
        ORDER BY timestamp
        LIMIT 3
    """).df()

    if len(cet_sample) > 0:
        log("  CET timezone conversion verification:")
        for _, row in cet_sample.iterrows():
            utc_ts = str(row['utc_timestamp'])[:23]
            cet_ts = str(row['cet_timestamp'])[:23]
            log(f"    UTC: {utc_ts} (hour {int(row['utc_hour']):02d}) -> CET: {cet_ts} (hour {int(row['cet_hour']):02d}) | session_date: {row['session_date']}")


def resolve_cp_column(col_names, candidates):
    """Return first matching column name from candidates, or None."""
    return next((c for c in candidates if c in col_names), None)


def export_parquet_files(con, output_dir):
    """Export all Parquet files for reporting."""
    log("Exporting Parquet files...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Raw data export
    raw_file = output_dir / 'events_raw.parquet'
    if raw_file.exists():
        raw_file.unlink()
    con.execute(f"COPY events TO '{raw_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    raw_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{raw_file}')").df()['n'][0]
    raw_size = os.path.getsize(raw_file) / (1024 * 1024)
    log(f"  events_raw.parquet ({raw_count:,} rows, {raw_size:.1f} MB)")

    # Anonymized data export (hash GPNs, drop emails)
    anonymized_file = output_dir / 'events_anonymized.parquet'
    if anonymized_file.exists():
        anonymized_file.unlink()

    events_schema = con.execute("DESCRIBE events").df()
    all_cols = events_schema['column_name'].tolist()
    hash_columns = {'gpn', 'CP_GPN'}
    drop_columns = {'email', 'CP_Email'}

    cols_to_hash = [c for c in all_cols if c in hash_columns]
    cols_to_drop = [c for c in all_cols if c in drop_columns]
    cols_kept = [c for c in all_cols if c not in drop_columns]

    select_parts = []
    for c in cols_kept:
        if c in hash_columns:
            alias = c.replace('gpn', 'person_hash').replace('GPN', 'Person_Hash')
            select_parts.append(f"sha256(CAST({c} AS VARCHAR))::VARCHAR AS {alias}")
        else:
            select_parts.append(c)

    select_sql = ', '.join(select_parts)
    con.execute(f"COPY (SELECT {select_sql} FROM events) TO '{anonymized_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")

    changes = []
    if cols_to_hash:
        changes.append(f"hashed: {', '.join(cols_to_hash)}")
    if cols_to_drop:
        changes.append(f"dropped: {', '.join(cols_to_drop)}")
    log(f"  events_anonymized.parquet ({raw_count:,} rows, {'; '.join(changes) or 'no changes'})")

    anonymized_size = os.path.getsize(anonymized_file) / (1024 * 1024)
    log(f"  events_anonymized.parquet size: {anonymized_size:.1f} MB")


def export_cdm_tables(con, output_dir):
    """Export CDM star-schema dimension and fact tables as Parquet files."""
    log("\nExporting CDM star-schema tables...")

    cdm_dir = Path(output_dir) / 'cdm'
    cdm_dir.mkdir(parents=True, exist_ok=True)

    # Discover available columns in the events table
    events_cols = con.execute("DESCRIBE events").df()['column_name'].tolist()

    # Resolve CP_ column name variants
    site_id_col = resolve_cp_column(events_cols, ['CP_SiteID', 'CP_siteID', 'CP_SiteId'])
    site_name_col = resolve_cp_column(events_cols, ['CP_SiteName', 'CP_siteName'])
    page_id_col = resolve_cp_column(events_cols, ['CP_PageId', 'CP_pageId', 'CP_PageID'])
    page_name_col = resolve_cp_column(events_cols, ['CP_PageName', 'CP_pageName'])
    page_url_col = resolve_cp_column(events_cols, ['CP_PageURL', 'CP_pageURL', 'CP_PageUrl'])
    content_type_col = resolve_cp_column(events_cols, ['CP_ContentType', 'CP_contentType'])
    page_status_col = resolve_cp_column(events_cols, ['CP_PageStatus', 'CP_pageStatus'])
    link_type_col = resolve_cp_column(events_cols, ['CP_Link_Type', 'CP_link_type', 'CP_LinkType'])
    component_col = resolve_cp_column(events_cols, ['CP_ComponentName', 'CP_componentName'])
    link_address_col = resolve_cp_column(events_cols, ['CP_Link_address', 'CP_link_address'])
    country_col = resolve_cp_column(events_cols, ['client_CountryOrRegion', 'client_countryOrRegion', 'client_countryorregion'])
    link_label_col = resolve_cp_column(events_cols, ['CP_Link_label', 'CP_link_label'])
    file_name_col = resolve_cp_column(events_cols, ['CP_FileName_Label', 'CP_fileName_Label'])
    file_type_col = resolve_cp_column(events_cols, ['CP_FileType_Label', 'CP_fileType_Label'])

    # Check which HR fields are available
    hr_fields = ['hr_division', 'hr_unit', 'hr_area', 'hr_sector', 'hr_segment',
                 'hr_function', 'hr_ou_code', 'hr_country', 'hr_region',
                 'hr_job_title', 'hr_job_family', 'hr_management_level', 'hr_cost_center']
    available_hr = [f for f in hr_fields if f in events_cols]
    has_hr = len(available_hr) > 0

    # Helper to build a safe column expression
    def col_expr(col_name, default="''"):
        return f'COALESCE("{col_name}", {default})' if col_name else default

    # ── dim_date ──
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dim_date AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY date_value) AS date_key,
            date_value,
            YEAR(date_value) AS year,
            QUARTER(date_value) AS quarter,
            MONTH(date_value) AS month,
            MONTHNAME(date_value) AS month_name,
            WEEKOFYEAR(date_value) AS week,
            ISODOW(date_value) AS day_of_week,
            DAYNAME(date_value) AS day_name,
            CASE WHEN ISODOW(date_value) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM (
            SELECT DISTINCT session_date AS date_value
            FROM events
            WHERE session_date IS NOT NULL
        )
    """)

    # ── dim_organization ──
    if has_hr:
        hash_parts = " || '|' || ".join([f"COALESCE({f}, '')" for f in available_hr])
        hr_select = ', '.join([f'{f} AS {f.replace("hr_", "")}' for f in available_hr])
        # Pad missing HR fields with NULL
        missing_hr = [f for f in hr_fields if f not in available_hr]
        null_parts = ', '.join([f"NULL AS {f.replace('hr_', '')}" for f in missing_hr])
        full_select = hr_select + (', ' + null_parts if null_parts else '')
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE dim_organization AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY org_hash) AS org_key,
                org_hash,
                {full_select}
            FROM (
                SELECT DISTINCT
                    MD5({hash_parts}) AS org_hash,
                    {', '.join(available_hr)}
                FROM events
            )
        """)
    else:
        null_cols = ', '.join([f"NULL AS {f.replace('hr_', '')}" for f in hr_fields])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE dim_organization AS
            SELECT 1 AS org_key, '' AS org_hash, {null_cols}
        """)

    # ── dim_site ──
    sid_expr = col_expr(site_id_col)
    sname_expr = col_expr(site_name_col)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_site AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY site_id, site_name) AS site_key,
            site_id, site_name
        FROM (
            SELECT DISTINCT
                {sid_expr} AS site_id,
                {sname_expr} AS site_name
            FROM events
        )
    """)

    # ── dim_page ──
    pid_expr = col_expr(page_id_col)
    pname_expr = col_expr(page_name_col)
    purl_expr = col_expr(page_url_col)
    ctype_expr = col_expr(content_type_col)
    pstat_expr = col_expr(page_status_col)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_page AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY page_id, page_name) AS page_key,
            page_id, page_name, page_url, content_type, page_status
        FROM (
            SELECT DISTINCT
                {pid_expr} AS page_id,
                {pname_expr} AS page_name,
                {purl_expr} AS page_url,
                {ctype_expr} AS content_type,
                {pstat_expr} AS page_status
            FROM events
        )
    """)

    # ── dim_link_type ──
    lt_expr = col_expr(link_type_col, "'(unknown)'")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_link_type AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY link_type) AS link_type_key,
            link_type
        FROM (
            SELECT DISTINCT {lt_expr} AS link_type FROM events
        )
    """)

    # ── dim_component ──
    comp_expr = col_expr(component_col, "'(unknown)'")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_component AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY component_name) AS component_key,
            component_name
        FROM (
            SELECT DISTINCT {comp_expr} AS component_name FROM events
        )
    """)

    # ── fact_clicks ──
    # Build org hash expression for joining
    if has_hr:
        org_hash_expr = "MD5(" + " || '|' || ".join([f"COALESCE(e.{f}, '')" for f in available_hr]) + ")"
        org_join = f"LEFT JOIN dim_organization dorg ON {org_hash_expr} = dorg.org_hash"
    else:
        org_join = "CROSS JOIN dim_organization dorg"

    # Build column expressions for optional CP_ fields
    la_expr = f'e."{link_address_col}" AS link_address' if link_address_col else "NULL AS link_address"
    ll_expr = f'e."{link_label_col}" AS link_label' if link_label_col else "NULL AS link_label"
    fn_expr = f'e."{file_name_col}" AS file_name' if file_name_col else "NULL AS file_name"
    ft_expr = f'e."{file_type_col}" AS file_type' if file_type_col else "NULL AS file_type"
    cr_expr = f'e."{country_col}" AS client_country' if country_col else "NULL AS client_country"

    # Site join expressions
    sid_join = f'COALESCE(e."{site_id_col}", \'\')' if site_id_col else "''"
    sname_join = f'COALESCE(e."{site_name_col}", \'\')' if site_name_col else "''"

    # Page join expressions
    pid_join = f'COALESCE(e."{page_id_col}", \'\')' if page_id_col else "''"
    pname_join = f'COALESCE(e."{page_name_col}", \'\')' if page_name_col else "''"
    purl_join = f'COALESCE(e."{page_url_col}", \'\')' if page_url_col else "''"
    ctype_join = f'COALESCE(e."{content_type_col}", \'\')' if content_type_col else "''"
    pstat_join = f'COALESCE(e."{page_status_col}", \'\')' if page_status_col else "''"

    # Link type / component join expressions
    lt_join = f"COALESCE(e.\"{link_type_col}\", '(unknown)')" if link_type_col else "'(unknown)'"
    comp_join = f"COALESCE(e.\"{component_col}\", '(unknown)')" if component_col else "'(unknown)'"

    # GPN anonymization — SHA-256 hash, same as events_anonymized
    gpn_hash_expr = "sha256(CAST(e.gpn AS VARCHAR))::VARCHAR" if 'gpn' in events_cols else "NULL"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fact_clicks AS
        SELECT
            dd.date_key,
            dorg.org_key,
            ds.site_key,
            dp.page_key,
            dlt.link_type_key,
            dc.component_key,
            {gpn_hash_expr} AS person_hash,
            e.user_id,
            e.session_id,
            e.session_key,
            e.timestamp,
            e.timestamp_cet,
            e.event_order,
            e.prev_event,
            e.ms_since_prev_event,
            e.sec_since_prev_event,
            e.time_since_prev_bucket,
            e.event_hour,
            e.event_weekday,
            {la_expr},
            {ll_expr},
            {fn_expr},
            {ft_expr},
            e.name AS event_name,
            {cr_expr}
        FROM events e
        LEFT JOIN dim_date dd ON e.session_date = dd.date_value
        {org_join}
        LEFT JOIN dim_site ds
            ON {sid_join} = ds.site_id AND {sname_join} = ds.site_name
        LEFT JOIN dim_page dp
            ON {pid_join} = dp.page_id AND {pname_join} = dp.page_name
            AND {purl_join} = dp.page_url AND {ctype_join} = dp.content_type
            AND {pstat_join} = dp.page_status
        LEFT JOIN dim_link_type dlt ON {lt_join} = dlt.link_type
        LEFT JOIN dim_component dc ON {comp_join} = dc.component_name
    """)

    # Export all CDM tables to Parquet
    cdm_tables = ['dim_date', 'dim_organization', 'dim_site', 'dim_page',
                  'dim_link_type', 'dim_component', 'fact_clicks']

    for table in cdm_tables:
        out_file = cdm_dir / f'{table}.parquet'
        if out_file.exists():
            out_file.unlink()
        con.execute(f"COPY {table} TO '{out_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        size_mb = os.path.getsize(out_file) / (1024 * 1024)
        log(f"  {table}.parquet ({row_count:,} rows, {size_mb:.1f} MB)")

    log("CDM export complete.")


def print_summary(con, output_dir=None):
    """Print comprehensive processing summary."""
    log("")
    log("=" * 64)
    log("  PROCESSING SUMMARY")
    log("=" * 64)

    # --- Processed files manifest ---
    tables = con.execute("SHOW TABLES").df()['name'].tolist()
    if 'processed_files' in tables:
        manifest = con.execute("""
            SELECT filename, row_count, processed_at, date_suffix
            FROM processed_files
            ORDER BY date_suffix, filename
        """).df()
        if len(manifest) > 0:
            log("\n  PROCESSED FILES")
            log("  " + "-" * 60)
            for _, row in manifest.iterrows():
                ts = str(row['processed_at'])[:19] if row['processed_at'] else '?'
                rows = f"{int(row['row_count']):,}" if row['row_count'] else '?'
                log(f"    {row['filename']:<45s} {rows:>8s} rows  (at {ts})")

    # --- DuckDB tables ---
    log("\n  DATABASE TABLES")
    log("  " + "-" * 60)
    for table in sorted(tables):
        if table.startswith('temp'):
            continue
        row_count = con.execute(f"SELECT COUNT(*) as n FROM {table}").df()['n'][0]
        col_count = len(con.execute(f"DESCRIBE {table}").df())
        log(f"    {table:<30s} {row_count:>10,} rows  ({col_count} columns)")

    # --- Parquet files ---
    if output_dir:
        parquet_files = sorted(Path(output_dir).glob('*.parquet'))
        if parquet_files:
            log("\n  PARQUET FILES EXPORTED")
            log("  " + "-" * 60)
            for pf in parquet_files:
                size_mb = os.path.getsize(pf) / (1024 * 1024)
                log(f"    {pf.name:<40s} ({size_mb:.1f} MB)")

        cdm_dir = Path(output_dir) / 'cdm'
        cdm_files = sorted(cdm_dir.glob('*.parquet')) if cdm_dir.exists() else []
        if cdm_files:
            log("\n  CDM STAR-SCHEMA FILES")
            log("  " + "-" * 60)
            for pf in cdm_files:
                size_mb = os.path.getsize(pf) / (1024 * 1024)
                log(f"    cdm/{pf.name:<36s} ({size_mb:.1f} MB)")

    # --- Date range & volume ---
    overview = con.execute("""
        SELECT
            MIN(session_date) as first_date,
            MAX(session_date) as last_date,
            COUNT(DISTINCT session_date) as days,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_key) as unique_sessions,
            COUNT(DISTINCT gpn) as unique_gpns
        FROM events
    """).df().iloc[0]

    log("\n  DATA OVERVIEW")
    log("  " + "-" * 60)
    if overview['first_date'] is not None:
        log(f"    Date range:        {overview['first_date']} to {overview['last_date']} ({int(overview['days'])} days)")
    log(f"    Total events:      {int(overview['total_events']):,}")
    log(f"    Unique users:      {int(overview['unique_users']):,}")
    log(f"    Unique sessions:   {int(overview['unique_sessions']):,}")
    log(f"    Unique GPNs:       {int(overview['unique_gpns']):,}")

    # --- HR join coverage ---
    events_cols = con.execute("DESCRIBE events").df()['column_name'].tolist()
    if 'hr_division' in events_cols:
        hr_coverage = con.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(hr_division) as with_hr_data,
                COUNT(gpn) as with_gpn
            FROM events
        """).df().iloc[0]

        total = int(hr_coverage['total'])
        with_hr = int(hr_coverage['with_hr_data'])
        with_gpn = int(hr_coverage['with_gpn'])

        log("\n  HR JOIN COVERAGE")
        log("  " + "-" * 60)
        log(f"    Events with GPN:       {with_gpn:>8,} / {total:,}  ({100.0 * with_gpn / total if total > 0 else 0:.1f}%)")
        log(f"    Events with HR data:   {with_hr:>8,} / {total:,}  ({100.0 * with_hr / total if total > 0 else 0:.1f}%)")

        divisions = con.execute("""
            SELECT hr_division, COUNT(*) as cnt
            FROM events
            WHERE hr_division IS NOT NULL
            GROUP BY hr_division
            ORDER BY cnt DESC
            LIMIT 10
        """).df()
        if len(divisions) > 0:
            log("\n    Top divisions:")
            for _, row in divisions.iterrows():
                log(f"      {str(row['hr_division']):<40s} {int(row['cnt']):>8,}")

        # Show unmatched GPNs (have GPN but no HR data)
        if with_gpn > with_hr:
            unmatched = con.execute("""
                SELECT gpn, COUNT(*) as cnt
                FROM events
                WHERE gpn IS NOT NULL AND hr_division IS NULL
                GROUP BY gpn
                ORDER BY cnt DESC
                LIMIT 15
            """).df()
            if len(unmatched) > 0:
                log(f"\n    Unmatched GPNs ({with_gpn - with_hr:,} events from {len(unmatched)} GPNs shown, may be more):")
                for _, row in unmatched.iterrows():
                    log(f"      {row['gpn']:<12s} ({int(row['cnt']):,} events)")

    # --- Field coverage ---
    log("\n  FIELD COVERAGE (non-null values)")
    log("  " + "-" * 60)
    total = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    check_fields = ['gpn', 'email', 'session_id', 'user_id']
    # Add any HR fields
    for col in events_cols:
        if col.startswith('hr_'):
            check_fields.append(col)
    # Add any CP_ fields
    cp_fields = [c for c in events_cols if c.startswith('CP_')]
    check_fields.extend(cp_fields[:15])  # Show first 15 CP fields

    for field in check_fields:
        if field in events_cols:
            val = con.execute(f'SELECT COUNT("{field}") FROM events').fetchone()[0]
            pct = 100.0 * val / total if total > 0 else 0
            bar = "#" * int(pct / 5) if pct > 0 else ""
            log(f"    {field:<35s} {val:>8,} / {total:,}  ({pct:5.1f}%)  {bar}")

    # --- Event name breakdown ---
    events_df = con.execute("""
        SELECT name, COUNT(*) as cnt,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
        FROM events
        GROUP BY name
        ORDER BY cnt DESC
    """).df()

    log("\n  EVENT TYPES")
    log("  " + "-" * 60)
    for _, row in events_df.iterrows():
        log(f"    {row['name']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    # --- Link type breakdown ---
    link_type_col = next((c for c in ['CP_Link_Type', 'CP_link_type', 'CP_LinkType'] if c in events_cols), None)
    if link_type_col:
        lt_df = con.execute(f"""
            SELECT COALESCE("{link_type_col}", '(blank)') as link_type, COUNT(*) as cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
            FROM events
            GROUP BY 1
            ORDER BY cnt DESC
        """).df()

        log("\n  LINK TYPES (CP_Link_Type)")
        log("  " + "-" * 60)
        for _, row in lt_df.iterrows():
            log(f"    {row['link_type']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    # --- Component breakdown ---
    comp_col = next((c for c in ['CP_ComponentName', 'CP_componentName'] if c in events_cols), None)
    if comp_col:
        comp_df = con.execute(f"""
            SELECT COALESCE("{comp_col}", '(blank)') as component, COUNT(*) as cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
            FROM events
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 10
        """).df()

        log("\n  TOP COMPONENTS (CP_ComponentName)")
        log("  " + "-" * 60)
        for _, row in comp_df.iterrows():
            log(f"    {row['component']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    # --- Site breakdown ---
    site_col = next((c for c in ['CP_SiteName', 'CP_siteName'] if c in events_cols), None)
    if site_col:
        site_df = con.execute(f"""
            SELECT COALESCE("{site_col}", '(blank)') as site_name, COUNT(*) as cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
            FROM events
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 10
        """).df()

        log("\n  TOP SITES (CP_SiteName)")
        log("  " + "-" * 60)
        for _, row in site_df.iterrows():
            log(f"    {row['site_name']:<35s} {int(row['cnt']):>8,}  ({row['pct']:.1f}%)")

    log("\n" + "=" * 64)


def process_clicks(input_file=None, full_refresh=False):
    """
    Main processing function.

    Args:
        input_file: Specific file to process, or None to auto-detect
        full_refresh: If True, delete DB and reprocess all files
    """
    script_dir = Path(__file__).parent
    input_dir = script_dir / 'input'
    data_dir = script_dir / 'data'
    output_dir = script_dir / 'output'
    db_path = data_dir / 'clicks.db'

    # HR history parquet from SearchAnalytics
    hr_parquet_path = script_dir.parent / 'SearchAnalytics' / 'output' / 'hr_history.parquet'

    # Create directories
    input_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("CLICKS ANALYTICS PROCESSING")
    log("=" * 60)

    # Handle full refresh
    if full_refresh:
        if db_path.exists():
            db_path.unlink()
            log("Full refresh: deleted existing database")

        files_to_process = get_all_input_files(input_dir)
        if not files_to_process:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            sys.exit(1)
        log(f"Full refresh: processing {len(files_to_process)} files")

        # Connect to DuckDB (fresh DB after deletion)
        con = duckdb.connect(str(db_path))
        ensure_manifest_table(con)

        for input_path in files_to_process:
            log(f"\nProcessing: {input_path.name}")
            file_hash = compute_file_hash(input_path)
            row_count = load_file_to_temp_table(con, input_path)
            log(f"  Loaded {row_count:,} rows")
            upsert_data(con)
            record_processed_file(con, input_path, file_hash, row_count)

    elif input_file:
        # Force-process a specific file (bypass delta check)
        input_path = Path(input_file)
        if not input_path.exists():
            log(f"ERROR: File not found: {input_file}")
            sys.exit(1)

        con = duckdb.connect(str(db_path))
        ensure_manifest_table(con)

        log(f"\nForce-processing: {input_path.name}")
        file_hash = compute_file_hash(input_path)
        row_count = load_file_to_temp_table(con, input_path)
        log(f"  Loaded {row_count:,} rows")
        upsert_data(con)
        record_processed_file(con, input_path, file_hash, row_count)

    else:
        # Default: delta mode — only process new or changed files
        all_files = get_all_input_files(input_dir)
        if not all_files:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            log("Supported formats: .xlsx, .xls, .csv")
            log("\nFilename format: clicks_export_YYYY_MM_DD.xlsx")
            log("Example filenames:")
            log("  clicks_export_2026_02_25.xlsx")
            log("  clicks_export_2026_02_25.csv")
            sys.exit(1)

        con = duckdb.connect(str(db_path))
        unprocessed = get_unprocessed_files(con, input_dir)

        if not unprocessed:
            log("All files already processed. Nothing new to do.")
            log("Use --full-refresh to reprocess everything.")
            con.close()
            return

        for input_path, file_hash, reason in unprocessed:
            log(f"\nProcessing ({reason}): {input_path.name}")
            row_count = load_file_to_temp_table(con, input_path)
            log(f"  Loaded {row_count:,} rows")
            upsert_data(con)
            record_processed_file(con, input_path, file_hash, row_count)

    # Load HR history for GPN-based join
    has_hr_history = load_hr_history(con, hr_parquet_path)

    # Add calculated columns (with HR join if available)
    add_calculated_columns(con, has_hr_history=has_hr_history)

    # Export Parquet files
    export_parquet_files(con, output_dir)

    # Export CDM star-schema tables
    export_cdm_tables(con, output_dir)

    # Print summary
    print_summary(con, output_dir)

    # Cleanup: drop tables that are re-created each run to reduce DB size
    log("\nCleaning up intermediate tables...")
    db_size_before = os.path.getsize(db_path) / (1024 * 1024)
    con.execute("DROP TABLE IF EXISTS hr_history")
    con.execute("DROP TABLE IF EXISTS events_raw")
    for cdm_table in ['dim_date', 'dim_organization', 'dim_site', 'dim_page',
                       'dim_link_type', 'dim_component', 'fact_clicks']:
        con.execute(f"DROP TABLE IF EXISTS {cdm_table}")
    con.execute("VACUUM")
    con.execute("CHECKPOINT")
    db_size_after = os.path.getsize(db_path) / (1024 * 1024)
    log(f"  Dropped hr_history, events_raw, and CDM temp tables; vacuumed database")
    log(f"  Database size: {db_size_before:.1f} MB -> {db_size_after:.1f} MB")

    log(f"\nDatabase: {db_path}")
    log(f"Parquet files: {output_dir}")

    con.close()
    log("\nDone!")


if __name__ == "__main__":
    full_refresh = '--full-refresh' in sys.argv

    input_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            input_file = arg
            break

    if len(sys.argv) == 1:
        print(__doc__)
        print("\nNo arguments provided - processing new/changed files (delta mode)\n")

    process_clicks(input_file=input_file, full_refresh=full_refresh)
