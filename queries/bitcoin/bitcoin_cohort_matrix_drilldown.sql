-- ============================================================
-- Query: Bitcoin Cohort Matrix Drilldown (2-Level Nested Query)
-- Description: Filters the cohort matrix to a single cohort via
--              {{cohort_filter}} and zero-fills the day × score_band
--              grid for gap-free dashboard charting.
--              2-level nesting: drilldown → cohort_matrix → base.
-- Author: stefanopepe
-- Created: 2026-02-10
-- Updated: 2026-02-10
-- Architecture: 2-level nested query
--   Level 1: query_6663464 (bitcoin_human_factor_cohort_matrix)
--   Level 0: query_6638509 (bitcoin_tx_features_daily)
-- ============================================================
-- Parameters:
--   {{cohort_filter}} - Cohort to drill into (dropdown)
--       Valid values:
--         'Shrimps (<1 BTC)'
--         'Crab (1-10 BTC)'
--         'Octopus (10-50 BTC)'
--         'Fish (50-100 BTC)'
--         'Dolphin (100-500 BTC)'
--         'Shark (500-1,000 BTC)'
--         'Whale (1,000-5,000 BTC)'
--         'Humpback (>5,000 BTC)'
--   {{start_date}} - Analysis start date (Dune date picker)
--   {{end_date}}   - Analysis end date (Dune date picker)
-- ============================================================
-- Output Columns:
--   day                     - Date (dense: every day in range)
--   score_band              - Score range (dense: all 10 bands per day)
--   score_band_order        - Numeric ordering (1-10)
--   cohort                  - Filtered cohort name (constant)
--   cohort_order            - Filtered cohort order (constant)
--   tx_count                - Number of transactions (0 for missing cells)
--   btc_volume              - Total BTC moved (0.0 for missing cells)
--   avg_score               - Average score in segment (NULL for missing)
--   avg_fee_btc             - Average tx fee in BTC (0.0 for missing)
--   total_fee_btc           - Total fees paid in BTC (0.0 for missing)
--   tx_with_address_reuse   - Count of txs with address reuse (0 for missing)
--   tx_with_output_mismatch - Count of txs with output mismatch (0 for missing)
--   pct_address_reuse       - % of txs with address reuse (NULL for missing)
-- ============================================================
-- Dense Output Guarantee:
--   Exactly N_days × 10 rows, where N_days = datediff(start, end).
--   Counts/sums COALESCE to 0; averages/percentages COALESCE to NULL.
-- ============================================================

WITH
-- Date spine: one row per day in the requested range
days AS (
    SELECT day
    FROM UNNEST(SEQUENCE(
        CAST(substr('{{start_date}}', 1, 10) AS DATE),
        CAST(substr('{{end_date}}',   1, 10) AS DATE) - INTERVAL '1' DAY,
        INTERVAL '1' DAY
    )) AS t(day)
),

-- Score band spine: all 10 bands
score_bands AS (
    SELECT score_band, score_band_order
    FROM (VALUES
        ('0-10',   1),
        ('10-20',  2),
        ('20-30',  3),
        ('30-40',  4),
        ('40-50',  5),
        ('50-60',  6),
        ('60-70',  7),
        ('70-80',  8),
        ('80-90',  9),
        ('90-100', 10)
    ) AS t(score_band, score_band_order)
),

-- Dense grid: every (day, score_band) combination
spine AS (
    SELECT d.day, sb.score_band, sb.score_band_order
    FROM days d
    CROSS JOIN score_bands sb
),

-- Sparse data from the parent cohort matrix, filtered to one cohort
filtered AS (
    SELECT
        day,
        score_band,
        score_band_order,
        cohort,
        cohort_order,
        tx_count,
        btc_volume,
        avg_score,
        avg_fee_btc,
        total_fee_btc,
        tx_with_address_reuse,
        tx_with_output_mismatch,
        pct_address_reuse
    FROM query_6663464
    WHERE cohort = '{{cohort_filter}}'
)

-- Zero-fill: LEFT JOIN spine to filtered data
SELECT
    s.day,
    s.score_band,
    s.score_band_order,
    '{{cohort_filter}}' AS cohort,
    COALESCE(
        f.cohort_order,
        CASE '{{cohort_filter}}'
            WHEN 'Shrimps (<1 BTC)' THEN 1
            WHEN 'Crab (1-10 BTC)' THEN 2
            WHEN 'Octopus (10-50 BTC)' THEN 3
            WHEN 'Fish (50-100 BTC)' THEN 4
            WHEN 'Dolphin (100-500 BTC)' THEN 5
            WHEN 'Shark (500-1,000 BTC)' THEN 6
            WHEN 'Whale (1,000-5,000 BTC)' THEN 7
            WHEN 'Humpback (>5,000 BTC)' THEN 8
        END
    ) AS cohort_order,
    -- Counts and sums: COALESCE to 0
    COALESCE(f.tx_count, 0) AS tx_count,
    COALESCE(f.btc_volume, 0.0) AS btc_volume,
    -- Averages: NULL for missing cells (avg of zero obs is undefined)
    f.avg_score,
    -- Fee metrics
    COALESCE(f.avg_fee_btc, 0.0) AS avg_fee_btc,
    COALESCE(f.total_fee_btc, 0.0) AS total_fee_btc,
    -- Privacy counts: COALESCE to 0
    COALESCE(f.tx_with_address_reuse, 0) AS tx_with_address_reuse,
    COALESCE(f.tx_with_output_mismatch, 0) AS tx_with_output_mismatch,
    -- Percentage: NULL for missing cells
    f.pct_address_reuse
FROM spine s
LEFT JOIN filtered f
    ON s.day = f.day
    AND s.score_band_order = f.score_band_order
ORDER BY s.day, s.score_band_order
