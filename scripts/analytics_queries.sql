-- =============================================================================
-- Clicks Analytics — Useful SQL Queries
-- =============================================================================
-- Target: Postgres schema "clicks" (created by scripts/duckdb_to_postgres.py)
--
-- Two query styles are used throughout:
--   - Star schema:  fact_clicks JOIN dim_*   (clean, dimensional)
--   - Flat table:   events_flat              (faster for ad-hoc, all 59 cols)
--
-- Run individual queries in pgAdmin's Query Tool. Adjust date ranges, filter
-- values, and LIMITs as needed.
-- =============================================================================

SET search_path = clicks, public;


-- =============================================================================
-- 1. VOLUME & OVERVIEW
-- =============================================================================

-- 1.1 Headline KPIs for a date range
SELECT
    COUNT(*)                         AS total_clicks,
    COUNT(DISTINCT person_hash)      AS unique_users,
    COUNT(DISTINCT session_key)      AS sessions,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT session_key), 0), 1)
                                     AS clicks_per_session,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT person_hash), 0), 1)
                                     AS clicks_per_user,
    MIN(d.date_value)                AS first_day,
    MAX(d.date_value)                AS last_day
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_value BETWEEN '2026-01-01' AND '2026-12-31';


-- 1.2 Daily click volume + active users (trend)
SELECT
    d.date_value,
    d.day_name,
    COUNT(*)                    AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT f.session_key) AS sessions
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date_value, d.day_name
ORDER BY d.date_value;


-- 1.3 Weekly volume with WoW growth
WITH weekly AS (
    SELECT d.year, d.week,
           MIN(d.date_value) AS week_start,
           COUNT(*) AS clicks,
           COUNT(DISTINCT f.person_hash) AS users
    FROM fact_clicks f JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.week
)
SELECT
    week_start,
    clicks,
    users,
    clicks - LAG(clicks) OVER (ORDER BY week_start) AS clicks_wow_delta,
    ROUND(100.0 * (clicks - LAG(clicks) OVER (ORDER BY week_start))
          / NULLIF(LAG(clicks) OVER (ORDER BY week_start), 0), 1) AS clicks_wow_pct
FROM weekly
ORDER BY week_start;


-- 1.4 Hour-of-day x day-of-week heatmap
SELECT
    f.event_weekday,
    f.event_hour,
    COUNT(*) AS clicks
FROM fact_clicks f
GROUP BY f.event_weekday, f.event_hour
ORDER BY
    CASE f.event_weekday
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END,
    f.event_hour;


-- =============================================================================
-- 2. CONTENT & PAGE PERFORMANCE
-- =============================================================================

-- 2.1 Top pages by clicks
SELECT
    p.page_name,
    p.page_url,
    p.content_type,
    COUNT(*)                      AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT f.session_key) AS sessions
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
GROUP BY p.page_name, p.page_url, p.content_type
ORDER BY clicks DESC
LIMIT 50;


-- 2.2 Top sites
SELECT
    s.site_name,
    COUNT(*)                      AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT p.page_key)    AS active_pages
FROM fact_clicks f
JOIN dim_site s ON f.site_key = s.site_key
JOIN dim_page p ON f.page_key = p.page_key
GROUP BY s.site_name
ORDER BY clicks DESC;


-- 2.3 Pages with high engagement but low reach (hidden gems)
-- (high clicks-per-user, but few unique users)
SELECT
    p.page_name,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT f.person_hash), 0), 2) AS clicks_per_user
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
GROUP BY p.page_name
HAVING COUNT(DISTINCT f.person_hash) BETWEEN 5 AND 100
   AND COUNT(*) >= 50
ORDER BY clicks_per_user DESC
LIMIT 30;


-- 2.4 Content-type breakdown
SELECT
    COALESCE(p.content_type, '(none)') AS content_type,
    COUNT(*) AS clicks,
    COUNT(DISTINCT p.page_key) AS pages,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
GROUP BY p.content_type
ORDER BY clicks DESC;


-- =============================================================================
-- 3. LINKS & DOWNLOADS
-- =============================================================================

-- 3.1 Top downloaded files
SELECT
    f.file_name,
    f.file_type,
    COUNT(*) AS downloads,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
WHERE f.file_name IS NOT NULL
GROUP BY f.file_name, f.file_type
ORDER BY downloads DESC
LIMIT 30;


-- 3.2 Link-type distribution
SELECT
    lt.link_type,
    COUNT(*) AS clicks,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM fact_clicks f
JOIN dim_link_type lt ON f.link_type_key = lt.link_type_key
GROUP BY lt.link_type
ORDER BY clicks DESC;


-- 3.3 Outbound link domains (which external sites do users visit?)
SELECT
    regexp_replace(f.link_address, '^https?://([^/]+).*', '\1') AS domain,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
WHERE f.link_address ~ '^https?://'
GROUP BY domain
ORDER BY clicks DESC
LIMIT 30;


-- 3.4 Most-clicked link labels per page (where do users actually click?)
SELECT
    p.page_name,
    f.link_label,
    COUNT(*) AS clicks
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
WHERE f.link_label IS NOT NULL
GROUP BY p.page_name, f.link_label
HAVING COUNT(*) >= 10
ORDER BY p.page_name, clicks DESC;


-- =============================================================================
-- 4. ORGANISATIONAL / HR ANALYSIS
-- =============================================================================

-- 4.1 Clicks per division
SELECT
    o.division,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT f.person_hash), 0), 1) AS clicks_per_user
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
WHERE o.division IS NOT NULL
GROUP BY o.division
ORDER BY clicks DESC;


-- 4.2 Top pages within a specific division
SELECT
    p.page_name,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_organization o ON f.org_key = o.org_key
WHERE o.division = 'Group Functions'   -- adjust
GROUP BY p.page_name
ORDER BY clicks DESC
LIMIT 30;


-- 4.3 Country / region distribution
SELECT
    o.country,
    o.region,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
GROUP BY o.country, o.region
ORDER BY clicks DESC;


-- 4.4 Division x content-type matrix (who consumes what?)
SELECT
    o.division,
    p.content_type,
    COUNT(*) AS clicks
FROM fact_clicks f
JOIN dim_organization o ON f.org_key = o.org_key
JOIN dim_page p ON f.page_key = p.page_key
WHERE o.division IS NOT NULL AND p.content_type IS NOT NULL
GROUP BY o.division, p.content_type
ORDER BY o.division, clicks DESC;


-- =============================================================================
-- 5. SESSIONS & USER BEHAVIOUR
-- =============================================================================

-- 5.1 Session length distribution (clicks per session)
WITH sess AS (
    SELECT session_key, COUNT(*) AS clicks_in_session
    FROM fact_clicks
    GROUP BY session_key
)
SELECT
    CASE
        WHEN clicks_in_session = 1 THEN '1 click'
        WHEN clicks_in_session BETWEEN 2 AND 5 THEN '2-5 clicks'
        WHEN clicks_in_session BETWEEN 6 AND 20 THEN '6-20 clicks'
        WHEN clicks_in_session BETWEEN 21 AND 50 THEN '21-50 clicks'
        ELSE '50+ clicks'
    END AS bucket,
    COUNT(*) AS sessions,
    SUM(clicks_in_session) AS total_clicks
FROM sess
GROUP BY bucket
ORDER BY MIN(clicks_in_session);


-- 5.2 Time-since-previous-event distribution (engagement signal)
SELECT
    f.time_since_prev_bucket,
    COUNT(*) AS events
FROM fact_clicks f
WHERE f.time_since_prev_bucket IS NOT NULL
GROUP BY f.time_since_prev_bucket
ORDER BY MIN(f.ms_since_prev_event);


-- 5.3 Power users (top 1% by click volume)
WITH per_user AS (
    SELECT person_hash, COUNT(*) AS clicks
    FROM fact_clicks
    WHERE person_hash IS NOT NULL
    GROUP BY person_hash
),
ranked AS (
    SELECT *, NTILE(100) OVER (ORDER BY clicks) AS pct_bucket
    FROM per_user
)
SELECT person_hash, clicks
FROM ranked
WHERE pct_bucket = 100
ORDER BY clicks DESC
LIMIT 50;


-- 5.4 First-time vs returning users per day
WITH first_seen AS (
    SELECT person_hash, MIN(d.date_value) AS first_day
    FROM fact_clicks f JOIN dim_date d ON f.date_key = d.date_key
    WHERE person_hash IS NOT NULL
    GROUP BY person_hash
)
SELECT
    d.date_value,
    SUM(CASE WHEN fs.first_day = d.date_value THEN 1 ELSE 0 END) AS new_users,
    COUNT(DISTINCT f.person_hash) - SUM(CASE WHEN fs.first_day = d.date_value THEN 1 ELSE 0 END) AS returning_users
FROM fact_clicks f
JOIN dim_date d ON f.date_key = d.date_key
JOIN first_seen fs ON f.person_hash = fs.person_hash
GROUP BY d.date_value
ORDER BY d.date_value;


-- 5.5 Weekly retention cohort (users active this week AND next week)
WITH weekly_users AS (
    SELECT DISTINCT
        DATE_TRUNC('week', d.date_value)::date AS week_start,
        f.person_hash
    FROM fact_clicks f JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.person_hash IS NOT NULL
)
SELECT
    a.week_start,
    COUNT(DISTINCT a.person_hash)                                   AS active_users,
    COUNT(DISTINCT b.person_hash)                                   AS retained_next_week,
    ROUND(100.0 * COUNT(DISTINCT b.person_hash)
          / NULLIF(COUNT(DISTINCT a.person_hash), 0), 1)            AS retention_pct
FROM weekly_users a
LEFT JOIN weekly_users b
       ON a.person_hash = b.person_hash
      AND b.week_start = a.week_start + INTERVAL '7 days'
GROUP BY a.week_start
ORDER BY a.week_start;


-- =============================================================================
-- 6. NAVIGATION FLOW (events_flat — uses prev_event for transitions)
-- =============================================================================

-- 6.1 Top page-to-page transitions within sessions
SELECT
    prev.page_name AS from_page,
    curr.page_name AS to_page,
    COUNT(*) AS transitions
FROM events_flat e
JOIN dim_page prev ON prev.page_url = e.CP_PageURL  -- adjust if needed
-- This requires reconstructing prev page; easier via window function below
WHERE 1=0;

-- 6.1b Cleaner: page transitions via window function on events_flat
WITH ordered AS (
    SELECT
        session_key,
        event_order,
        "CP_PageName"  AS page_name,
        LAG("CP_PageName") OVER (PARTITION BY session_key ORDER BY event_order) AS prev_page_name
    FROM events_flat
)
SELECT
    prev_page_name AS from_page,
    page_name      AS to_page,
    COUNT(*)       AS transitions
FROM ordered
WHERE prev_page_name IS NOT NULL AND prev_page_name <> page_name
GROUP BY prev_page_name, page_name
ORDER BY transitions DESC
LIMIT 50;


-- 6.2 Bounce rate per page (sessions with only 1 page click)
WITH session_pages AS (
    SELECT session_key, COUNT(DISTINCT "CP_PageName") AS page_count,
           MIN("CP_PageName") AS entry_page
    FROM events_flat
    GROUP BY session_key
)
SELECT
    entry_page,
    COUNT(*) AS sessions,
    SUM(CASE WHEN page_count = 1 THEN 1 ELSE 0 END) AS single_page_sessions,
    ROUND(100.0 * SUM(CASE WHEN page_count = 1 THEN 1 ELSE 0 END)
          / COUNT(*), 1) AS bounce_rate_pct
FROM session_pages
WHERE entry_page IS NOT NULL
GROUP BY entry_page
HAVING COUNT(*) >= 20
ORDER BY sessions DESC
LIMIT 30;


-- =============================================================================
-- 7. VIDEO ANALYTICS (events_flat)
-- =============================================================================

-- 7.1 Top videos by play events
SELECT
    "CP_Video_Title" AS video,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play')     AS plays,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'pause')    AS pauses,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'ended')    AS completions,
    COUNT(DISTINCT gpn) AS viewers,
    ROUND(AVG("CP_Video_Duration"::numeric), 0) AS avg_duration_s
FROM events_flat
WHERE "CP_Video_Title" IS NOT NULL
GROUP BY "CP_Video_Title"
ORDER BY plays DESC
LIMIT 30;


-- 7.2 Video completion rate
SELECT
    "CP_Video_Title" AS video,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play')   AS plays,
    COUNT(*) FILTER (WHERE "CP_Video_Action" = 'ended')  AS completions,
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CP_Video_Action" = 'ended')
          / NULLIF(COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play'), 0), 1)
        AS completion_rate_pct
FROM events_flat
WHERE "CP_Video_Title" IS NOT NULL
GROUP BY "CP_Video_Title"
HAVING COUNT(*) FILTER (WHERE "CP_Video_Action" = 'play') >= 10
ORDER BY completion_rate_pct DESC;


-- =============================================================================
-- 8. DATA QUALITY & INVESTIGATIONS
-- =============================================================================

-- 8.1 HR-match coverage
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NOT NULL) AS matched,
    COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NULL)     AS gpn_no_hr,
    COUNT(*) FILTER (WHERE gpn IS NULL)                                 AS no_gpn,
    ROUND(100.0 * COUNT(*) FILTER (WHERE gpn IS NOT NULL AND hr_division IS NOT NULL)
          / COUNT(*), 1) AS match_rate_pct
FROM events_flat;


-- 8.2 GPNs that consistently fail HR-match (candidates for HR-history fix)
SELECT
    gpn,
    COUNT(*) AS events,
    MIN(session_date) AS first_seen,
    MAX(session_date) AS last_seen,
    ANY_VALUE(email) AS sample_email
FROM events_flat
WHERE gpn IS NOT NULL AND hr_division IS NULL
GROUP BY gpn
ORDER BY events DESC
LIMIT 50;


-- 8.3 GPN length anomalies (should always be 8 digits)
SELECT LENGTH(gpn) AS gpn_length, COUNT(DISTINCT gpn) AS distinct_gpns, COUNT(*) AS events
FROM events_flat
WHERE gpn IS NOT NULL
GROUP BY LENGTH(gpn)
ORDER BY gpn_length;


-- 8.4 Suspicious sessions (very high click-rate — possible bots/scripts)
WITH sess AS (
    SELECT
        session_key,
        gpn,
        COUNT(*) AS events,
        EXTRACT(EPOCH FROM (MAX(timestamp_cet) - MIN(timestamp_cet))) AS duration_s
    FROM events_flat
    GROUP BY session_key, gpn
)
SELECT
    session_key,
    gpn,
    events,
    ROUND(duration_s::numeric, 1) AS duration_s,
    ROUND(events::numeric / NULLIF(duration_s, 0), 2) AS clicks_per_sec
FROM sess
WHERE duration_s > 0
  AND events >= 50
  AND events::numeric / NULLIF(duration_s, 0) > 1.0   -- >1 click/sec sustained
ORDER BY clicks_per_sec DESC
LIMIT 50;


-- 8.5 Pages where >30% of clicks are <0.5s after the previous (possible mis-clicks/double-clicks)
SELECT
    "CP_PageName" AS page_name,
    COUNT(*) AS clicks,
    SUM(CASE WHEN time_since_prev_bucket = '< 0.5s' THEN 1 ELSE 0 END) AS fast_clicks,
    ROUND(100.0 * SUM(CASE WHEN time_since_prev_bucket = '< 0.5s' THEN 1 ELSE 0 END)
          / COUNT(*), 1) AS fast_pct
FROM events_flat
WHERE "CP_PageName" IS NOT NULL
GROUP BY "CP_PageName"
HAVING COUNT(*) >= 100
   AND SUM(CASE WHEN time_since_prev_bucket = '< 0.5s' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 30
ORDER BY fast_pct DESC;


-- 8.6 Find a specific user's full journey (replace person_hash)
SELECT
    f.timestamp_cet,
    p.page_name,
    f.link_label,
    f.link_address,
    lt.link_type,
    f.event_order
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_link_type lt ON f.link_type_key = lt.link_type_key
WHERE f.person_hash = '<paste-hash-here>'
ORDER BY f.timestamp_cet
LIMIT 500;


-- 8.7 Find a specific page's audience profile
SELECT
    o.division,
    o.country,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users
FROM fact_clicks f
JOIN dim_page p ON f.page_key = p.page_key
JOIN dim_organization o ON f.org_key = o.org_key
WHERE p.page_name ILIKE '%annual report%'   -- adjust
GROUP BY o.division, o.country
ORDER BY clicks DESC;


-- =============================================================================
-- 9. CAMPAIGN / TARGETING ANALYSIS
-- =============================================================================

-- 9.1 Theme performance
SELECT
    th.theme,
    COUNT(*) AS clicks,
    COUNT(DISTINCT f.person_hash) AS users,
    COUNT(DISTINCT f.page_key) AS pages
FROM fact_clicks f
JOIN dim_theme th ON f.theme_key = th.theme_key
WHERE th.theme NOT IN ('(none)', '(unknown)')
GROUP BY th.theme
ORDER BY clicks DESC;


-- 9.2 Topic x division (which topics resonate where?)
SELECT
    t.topic,
    o.division,
    COUNT(*) AS clicks
FROM fact_clicks f
JOIN dim_topic t ON f.topic_key = t.topic_key
JOIN dim_organization o ON f.org_key = o.org_key
WHERE t.topic NOT IN ('(none)', '(unknown)') AND o.division IS NOT NULL
GROUP BY t.topic, o.division
ORDER BY t.topic, clicks DESC;


-- 9.3 Targeted-vs-actual reach (target audience vs who actually clicked)
SELECT
    tg.target_org      AS targeted_org,
    o.division         AS actual_division,
    COUNT(*)           AS clicks,
    CASE WHEN tg.target_org = o.division THEN 'on-target' ELSE 'off-target' END AS hit
FROM fact_clicks f
JOIN dim_target_org tg ON f.target_org_key = tg.target_org_key
JOIN dim_organization o ON f.org_key = o.org_key
WHERE tg.target_org NOT IN ('(none)', '(unknown)')
GROUP BY tg.target_org, o.division
ORDER BY tg.target_org, clicks DESC;


-- =============================================================================
-- 10. UTILITY: SCHEMA EXPLORATION
-- =============================================================================

-- 10.1 Row counts across all tables
SELECT 'fact_clicks'        AS tbl, COUNT(*) AS n FROM fact_clicks
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
UNION ALL SELECT 'events_flat',       COUNT(*) FROM events_flat
ORDER BY n DESC;


-- 10.2 Date coverage
SELECT MIN(date_value) AS first_day, MAX(date_value) AS last_day,
       COUNT(*) AS distinct_days
FROM dim_date
WHERE date_key IN (SELECT DISTINCT date_key FROM fact_clicks);


-- 10.3 Top values per dimension (sanity check after a fresh load)
SELECT 'site' AS dim, site_name AS value, NULL::text AS extra FROM dim_site LIMIT 5
UNION ALL SELECT 'link_type', link_type, NULL FROM dim_link_type LIMIT 5
UNION ALL SELECT 'theme', theme, NULL FROM dim_theme LIMIT 5;
