-- =============================================================================
-- Clicks Analytics — Reusable Views
-- =============================================================================
-- Run this file once against the "clicks" database to create all views, then
-- query them with arbitrary WHERE clauses, e.g.:
--
--   SELECT * FROM clicks.v_daily_clicks WHERE date_value >= '2026-04-01';
--   SELECT * FROM clicks.v_page_performance WHERE division = 'Group Functions'
--     ORDER BY clicks DESC LIMIT 20;
--
-- All views live in schema "clicks" with prefix "v_".
-- Views are raw aggregates without LIMIT/ORDER BY — apply those at query time.
-- =============================================================================

SET search_path = clicks, public;

-- Drop in dependency-safe order (cascade is fine since nothing depends on these
-- outside this file).
DROP VIEW IF EXISTS
    v_daily_clicks, v_weekly_clicks, v_hour_weekday_heatmap,
    v_page_performance, v_site_performance, v_content_type,
    v_link_type_distribution, v_outbound_domains, v_link_label_per_page,
    v_downloads,
    v_division_metrics, v_country_region, v_division_content_type,
    v_session_metrics, v_user_metrics, v_user_first_seen, v_daily_new_returning,
    v_session_transitions, v_bounce_rate,
    v_video_metrics,
    v_data_quality_match, v_unmatched_gpns, v_gpn_length, v_suspicious_sessions, v_fast_click_pages,
    v_theme_performance, v_topic_division, v_targeting_hits,
    v_table_rowcounts
CASCADE;


-- =============================================================================
-- 1. VOLUME & TIME-BASED VIEWS
-- =============================================================================

CREATE VIEW v_daily_clicks AS
SELECT
    d.date_value,
    d.year, d.quarter, d.month, d.week, d.day_name, d.is_weekend,
    COUNT(*)                       AS clicks,
    COUNT(DISTINCT f.person_hash)  AS users,
    COUNT(DISTINCT f.session_key)  AS sessions
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date_value, d.year, d.quarter, d.month, d.week, d.day_name, d.is_weekend;


CREATE VIEW v_weekly_clicks AS
WITH weekly AS (
    SELECT d.year, d.week,
           MIN(d.date_value) AS week_start,
           COUNT(*) AS clicks,
           COUNT(DISTINCT f.person_hash) AS users,
           COUNT(DISTINCT f.session_key) AS sessions
    FROM fact_clicks f JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.week
)
SELECT
    week_start, year, week, clicks, users, sessions,
    clicks - LAG(clicks) OVER (ORDER BY week_start)                              AS clicks_wow_delta,
    ROUND(100.0 * (clicks - LAG(clicks) OVER (ORDER BY week_start))
          / NULLIF(LAG(clicks) OVER (ORDER BY week_start), 0), 1)                AS clicks_wow_pct
FROM weekly;


CREATE VIEW v_hour_weekday_heatmap AS
SELECT
    f.event_weekday,
    CASE f.event_weekday
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END AS weekday_num,
    f.event_hour,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
GROUP BY f.event_weekday, f.event_hour;


-- =============================================================================
-- 2. CONTENT & PAGE VIEWS
-- =============================================================================

CREATE VIEW v_page_performance AS
SELECT
    p.page_key,
    p.page_name,
    p.page_url,
    p.content_type,
    p.page_status,
    s.site_name,
    o.division,
    o.country,
    d.date_value,
    COUNT(*)                       AS clicks,
    COUNT(DISTINCT f.person_hash)  AS users,
    COUNT(DISTINCT f.session_key)  AS sessions
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_site s ON f.site_key = s.site_key
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY p.page_key, p.page_name, p.page_url, p.content_type, p.page_status,
         s.site_name, o.division, o.country, d.date_value;


CREATE VIEW v_site_performance AS
SELECT
    s.site_key,
    s.site_name,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT f.page_key) AS active_pages
FROM fact_clicks f
JOIN dim_site s ON f.site_key = s.site_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY s.site_key, s.site_name, d.date_value;


CREATE VIEW v_content_type AS
SELECT
    COALESCE(p.content_type, '(none)') AS content_type,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT p.page_key) AS pages,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY p.content_type, d.date_value;


-- =============================================================================
-- 3. LINKS & DOWNLOADS
-- =============================================================================

CREATE VIEW v_link_type_distribution AS
SELECT
    lt.link_type,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_link_type lt ON f.link_type_key = lt.link_type_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY lt.link_type, d.date_value;


CREATE VIEW v_outbound_domains AS
SELECT
    regexp_replace(f.link_address, '^https?://([^/]+).*', '\1') AS domain,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.link_address ~ '^https?://'
GROUP BY domain, d.date_value;


CREATE VIEW v_link_label_per_page AS
SELECT
    p.page_name,
    f.link_label,
    d.date_value,
    COUNT(*) AS clicks
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.link_label IS NOT NULL
GROUP BY p.page_name, f.link_label, d.date_value;


CREATE VIEW v_downloads AS
SELECT
    f.file_name,
    f.file_type,
    p.page_name,
    o.division,
    d.date_value,
    COUNT(*) AS downloads,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.file_name IS NOT NULL
GROUP BY f.file_name, f.file_type, p.page_name, o.division, d.date_value;


-- =============================================================================
-- 4. ORGANISATIONAL VIEWS
-- =============================================================================

CREATE VIEW v_division_metrics AS
SELECT
    o.division,
    o.unit,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT f.person_hash), 0), 1) AS clicks_per_user
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY o.division, o.unit, d.date_value;


CREATE VIEW v_country_region AS
SELECT
    o.country,
    o.region,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY o.country, o.region, d.date_value;


CREATE VIEW v_division_content_type AS
SELECT
    o.division,
    p.content_type,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY o.division, p.content_type, d.date_value;


-- =============================================================================
-- 5. SESSIONS & USER BEHAVIOUR
-- =============================================================================

CREATE VIEW v_session_metrics AS
SELECT
    f.session_key,
    f.person_hash,
    o.division,
    o.country,
    MIN(d.date_value)         AS session_date,
    MIN(f.timestamp_cet)      AS session_start,
    MAX(f.timestamp_cet)      AS session_end,
    EXTRACT(EPOCH FROM (MAX(f.timestamp_cet) - MIN(f.timestamp_cet))) AS duration_s,
    COUNT(*)                  AS clicks,
    COUNT(DISTINCT f.page_key) AS distinct_pages
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_organization o ON f.org_key = o.org_key
GROUP BY f.session_key, f.person_hash, o.division, o.country;


CREATE VIEW v_user_metrics AS
SELECT
    f.person_hash,
    o.division,
    o.country,
    o.management_level,
    MIN(d.date_value) AS first_seen,
    MAX(d.date_value) AS last_seen,
    COUNT(DISTINCT d.date_value) AS active_days,
    COUNT(DISTINCT f.session_key) AS sessions,
    COUNT(*) AS clicks
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_organization o ON f.org_key = o.org_key
WHERE f.person_hash IS NOT NULL
GROUP BY f.person_hash, o.division, o.country, o.management_level;


CREATE VIEW v_user_first_seen AS
SELECT
    f.person_hash,
    MIN(d.date_value) AS first_day
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.person_hash IS NOT NULL
GROUP BY f.person_hash;


CREATE VIEW v_daily_new_returning AS
SELECT
    d.date_value,
    SUM(CASE WHEN fs.first_day = d.date_value THEN 1 ELSE 0 END)                  AS new_users,
    COUNT(DISTINCT f.person_hash)
        - SUM(CASE WHEN fs.first_day = d.date_value THEN 1 ELSE 0 END)            AS returning_users,
    COUNT(DISTINCT f.person_hash)                                                 AS total_users
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
JOIN v_user_first_seen fs ON f.person_hash = fs.person_hash
GROUP BY d.date_value;


-- =============================================================================
-- 6. NAVIGATION FLOW (built on events_flat)
-- =============================================================================

CREATE VIEW v_session_transitions AS
WITH ordered AS (
    SELECT
        session_key,
        session_date,
        event_order,
        "CP_PageName"  AS page_name,
        LAG("CP_PageName") OVER (PARTITION BY session_key ORDER BY event_order) AS prev_page_name
    FROM events_flat
)
SELECT
    session_date,
    prev_page_name AS from_page,
    page_name      AS to_page,
    COUNT(*)       AS transitions
FROM ordered
WHERE prev_page_name IS NOT NULL AND prev_page_name <> page_name
GROUP BY session_date, prev_page_name, page_name;


CREATE VIEW v_bounce_rate AS
WITH session_pages AS (
    SELECT
        session_key,
        session_date,
        COUNT(DISTINCT "CP_PageName") AS page_count,
        MIN("CP_PageName") AS entry_page
    FROM events_flat
    GROUP BY session_key, session_date
)
SELECT
    entry_page,
    session_date,
    COUNT(*) AS sessions,
    SUM(CASE WHEN page_count = 1 THEN 1 ELSE 0 END) AS single_page_sessions,
    ROUND(100.0 * SUM(CASE WHEN page_count = 1 THEN 1 ELSE 0 END)
          / COUNT(*), 1) AS bounce_rate_pct
FROM session_pages
WHERE entry_page IS NOT NULL
GROUP BY entry_page, session_date;


-- =============================================================================
-- 7. VIDEO ANALYTICS
-- =============================================================================

CREATE VIEW v_video_metrics AS
SELECT
    "CP_Video_Title" AS video,
    "CP_Video_Id"    AS video_id,
    session_date,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play')     AS plays,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'pause')    AS pauses,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'ended')    AS completions,
    COUNT(DISTINCT gpn) AS viewers,
    ROUND(AVG("CP_Video_Duration"::numeric), 0) AS avg_duration_s,
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CP_Video_Action" = 'ended')
          / NULLIF(COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play'), 0), 1)
        AS completion_rate_pct
FROM events_flat
WHERE "CP_Video_Title" IS NOT NULL
GROUP BY "CP_Video_Title", "CP_Video_Id", session_date;


-- =============================================================================
-- 8. DATA QUALITY VIEWS
-- =============================================================================

CREATE VIEW v_data_quality_match AS
SELECT
    session_date,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NOT NULL) AS matched,
    COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NULL)     AS gpn_no_hr,
    COUNT(*) FILTER (WHERE gpn IS NULL)                                 AS no_gpn,
    ROUND(100.0 * COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NOT NULL)
          / NULLIF(COUNT(*), 0), 1) AS match_rate_pct
FROM events_flat
GROUP BY session_date;


CREATE VIEW v_unmatched_gpns AS
SELECT
    gpn,
    COUNT(*) AS events,
    MIN(session_date) AS first_seen,
    MAX(session_date) AS last_seen,
    MAX(email)        AS sample_email
FROM events_flat
WHERE gpn IS NOT NULL AND hr_division IS NULL
GROUP BY gpn;


CREATE VIEW v_gpn_length AS
SELECT
    LENGTH(gpn) AS gpn_length,
    COUNT(DISTINCT gpn) AS distinct_gpns,
    COUNT(*) AS events
FROM events_flat
WHERE gpn IS NOT NULL
GROUP BY LENGTH(gpn);


CREATE VIEW v_suspicious_sessions AS
SELECT
    session_key,
    session_date,
    gpn,
    COUNT(*) AS events,
    EXTRACT(EPOCH FROM (MAX(timestamp_cet) - MIN(timestamp_cet))) AS duration_s,
    ROUND(COUNT(*)::numeric / NULLIF(EXTRACT(EPOCH FROM (MAX(timestamp_cet) - MIN(timestamp_cet))), 0), 2)
        AS clicks_per_sec
FROM events_flat
GROUP BY session_key, session_date, gpn;


CREATE VIEW v_fast_click_pages AS
SELECT
    "CP_PageName" AS page_name,
    session_date,
    COUNT(*) AS clicks,
    SUM(CASE WHEN time_since_prev_bucket = '< 0.5s' THEN 1 ELSE 0 END) AS fast_clicks,
    ROUND(100.0 * SUM(CASE WHEN time_since_prev_bucket = '< 0.5s' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1) AS fast_pct
FROM events_flat
WHERE "CP_PageName" IS NOT NULL
GROUP BY "CP_PageName", session_date;


-- =============================================================================
-- 9. CAMPAIGN / TARGETING
-- =============================================================================

CREATE VIEW v_theme_performance AS
SELECT
    th.theme,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT f.page_key) AS pages
FROM fact_clicks f
JOIN dim_theme th ON f.theme_key = th.theme_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY th.theme, d.date_value;


CREATE VIEW v_topic_division AS
SELECT
    t.topic,
    o.division,
    d.date_value,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_topic t ON f.topic_key = t.topic_key
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY t.topic, o.division, d.date_value;


CREATE VIEW v_targeting_hits AS
SELECT
    tg.target_org      AS targeted_org,
    o.division         AS actual_division,
    d.date_value,
    COUNT(*)           AS clicks,
    CASE WHEN tg.target_org = o.division THEN 'on-target' ELSE 'off-target' END AS hit
FROM fact_clicks f
JOIN dim_target_org tg ON f.target_org_key = tg.target_org_key
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY tg.target_org, o.division, d.date_value;


-- =============================================================================
-- 10. UTILITY
-- =============================================================================

CREATE VIEW v_table_rowcounts AS
SELECT 'fact_clicks'        AS tbl, COUNT(*)::bigint AS n FROM fact_clicks
UNION ALL SELECT 'dim_date',          COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_organization',  COUNT(*) FROM dim_organization
UNION ALL SELECT 'dim_site',          COUNT(*) FROM dim_site
UNION ALL SELECT 'dim_page',          COUNT(*) FROM dim_page
UNION ALL SELECT 'dim_link_type',     COUNT(*) FROM dim_link_type
UNION ALL SELECT 'dim_component',     COUNT(*) FROM dim_component
UNION ALL SELECT 'dim_topic',         COUNT(*) FROM dim_topic
UNION ALL SELECT 'dim_theme',         COUNT(*) FROM dim_theme
UNION ALL SELECT 'dim_target_org',    COUNT(*) FROM dim_target_org
UNION ALL SELECT 'dim_target_region', COUNT(*) FROM dim_target_region
UNION ALL SELECT 'events_flat',       COUNT(*) FROM events_flat;


-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================
-- All views are designed so you add WHERE / GROUP BY / ORDER BY / LIMIT on top.
--
-- Date range:
--   SELECT * FROM v_daily_clicks
--   WHERE date_value BETWEEN '2026-04-01' AND '2026-04-30'
--   ORDER BY date_value;
--
-- Top pages in a division (current month):
--   SELECT page_name, SUM(clicks) AS clicks, SUM(users) AS users
--   FROM v_page_performance
--   WHERE division = 'Group Functions' AND date_value >= date_trunc('month', CURRENT_DATE)
--   GROUP BY page_name
--   ORDER BY clicks DESC LIMIT 30;
--
-- Top downloaded files all-time:
--   SELECT file_name, file_type, SUM(downloads) AS downloads, SUM(users) AS users
--   FROM v_downloads
--   GROUP BY file_name, file_type
--   ORDER BY downloads DESC LIMIT 30;
--
-- Bounce-rate hotspots:
--   SELECT entry_page,
--          SUM(sessions) AS sessions,
--          ROUND(100.0 * SUM(single_page_sessions) / SUM(sessions), 1) AS bounce_pct
--   FROM v_bounce_rate
--   GROUP BY entry_page
--   HAVING SUM(sessions) >= 50
--   ORDER BY bounce_pct DESC;
--
-- Suspicious bot-like sessions:
--   SELECT * FROM v_suspicious_sessions
--   WHERE events >= 50 AND clicks_per_sec > 1.0
--   ORDER BY clicks_per_sec DESC;
--
-- Targeting effectiveness:
--   SELECT targeted_org, hit, SUM(clicks) AS clicks
--   FROM v_targeting_hits
--   WHERE targeted_org NOT IN ('(none)','(unknown)')
--   GROUP BY targeted_org, hit
--   ORDER BY targeted_org, hit;
-- =============================================================================
