-- ============================================================
-- Query: Lending Loop Metrics Daily (Nested Query)
-- Description: Daily aggregated metrics for cross-protocol lending loops.
--              Provides high-level analytics on loop activity, credit creation,
--              and protocol transition patterns.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-05
-- Architecture: V2 Nested Query (aggregates loop_detection)
-- Dependencies: lending_loop_detection, lending_flow_stitching
-- ============================================================
-- Output Columns:
--   day                  - Date
--   loops_started        - New loops initiated
--   unique_loopers       - Distinct entities with loops
--   gross_credit_created_usd - Total borrowed across all loops
--   avg_recursion_depth  - Average hop depth
--   max_recursion_depth  - Deepest loop observed
--   protocol_pair        - Most common source->dest pair
--   protocol_pair_volume - Volume for most common pair
-- ============================================================

WITH
-- Reference the loop detection query (column-pruned)
loops AS (
    SELECT
        entity_address,
        start_date,
        recursion_depth,
        gross_borrowed_usd
    FROM query_<LOOP_DETECTION_QUERY_ID>
),

-- Reference the flow stitching query (column-pruned)
flows AS (
    SELECT
        block_date,
        source_protocol,
        dest_protocol,
        entity_address,
        amount_usd
    FROM query_6690272
),

-- ============================================================
-- DAILY LOOP AGGREGATES
-- ============================================================

daily_loop_stats AS (
    SELECT
        start_date AS day,
        COUNT(*) AS loops_started,
        COUNT(DISTINCT entity_address) AS unique_loopers,
        SUM(gross_borrowed_usd) AS gross_credit_created_usd,
        AVG(recursion_depth) AS avg_recursion_depth,
        MAX(recursion_depth) AS max_recursion_depth,
        -- Count by loop depth
        COUNT(*) FILTER (WHERE recursion_depth = 1) AS single_hop_loops,
        COUNT(*) FILTER (WHERE recursion_depth = 2) AS double_hop_loops,
        COUNT(*) FILTER (WHERE recursion_depth >= 3) AS deep_loops
    FROM loops
    GROUP BY start_date
),

-- ============================================================
-- PROTOCOL PAIR TRANSITION MATRIX (Daily)
-- ============================================================

protocol_pairs AS (
    SELECT
        block_date AS day,
        source_protocol,
        dest_protocol,
        CONCAT(source_protocol, ' -> ', dest_protocol) AS protocol_pair,
        COUNT(*) AS flow_count,
        SUM(COALESCE(amount_usd, 0)) AS flow_volume_usd,
        COUNT(DISTINCT entity_address) AS unique_entities
    FROM flows
    GROUP BY
        block_date,
        source_protocol,
        dest_protocol
),

-- Get the top protocol pair per day
top_pairs AS (
    SELECT
        day,
        protocol_pair,
        flow_volume_usd AS protocol_pair_volume,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY flow_volume_usd DESC) AS rn
    FROM protocol_pairs
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================

SELECT
    d.day,
    d.loops_started,
    d.unique_loopers,
    d.gross_credit_created_usd,
    ROUND(d.avg_recursion_depth, 2) AS avg_recursion_depth,
    d.max_recursion_depth,
    d.single_hop_loops,
    d.double_hop_loops,
    d.deep_loops,
    t.protocol_pair AS top_protocol_pair,
    t.protocol_pair_volume AS top_pair_volume_usd
FROM daily_loop_stats d
LEFT JOIN top_pairs t
    ON t.day = d.day
    AND t.rn = 1
ORDER BY d.day DESC
