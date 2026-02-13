-- ============================================================
-- Query: Lending Loop Detection (Nested Query)
-- Description: Detects multi-hop lending loops where entities
--              chain borrow->supply flows across protocols.
--              Uses window functions for single-pass detection
--              with uniform temporal constraint (1-hour window).
--              Handles arbitrary hop depth (not limited to 3).
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-13
-- Architecture: V2 Nested Query (builds on flow_stitching)
-- Dependencies: lending_flow_stitching
-- ============================================================
-- Output Columns:
--   loop_id              - Unique loop identifier (root flow_id)
--   entity_address       - Entity executing the loop
--   start_date           - Loop initiation date
--   end_date             - Always NULL (repay tracking not implemented)
--   protocols_involved   - Array of protocols in loop path
--   hop_count            - Number of protocol hops
--   recursion_depth      - Max depth of recursive borrowing (= hop_count)
--   root_tx_hash         - First transaction in loop
--   gross_borrowed_usd   - Total USD borrowed across all hops
--   loop_status          - deep_loop / standard_loop / single_hop
-- ============================================================

WITH
-- Reference the flow stitching query (now materialized)
flows AS (
    SELECT
        flow_id,
        block_date,
        entity_address,
        source_protocol,
        dest_protocol,
        borrow_tx_hash,
        borrow_time,
        amount_usd
    FROM query_6690272
),

-- ============================================================
-- CHAIN DETECTION VIA WINDOW FUNCTIONS
-- Order flows per entity and check if each flow continues
-- from the previous flow's destination protocol within 1 hour
-- ============================================================

flows_with_prev AS (
    SELECT
        flow_id,
        block_date,
        entity_address,
        source_protocol,
        dest_protocol,
        borrow_tx_hash,
        borrow_time,
        amount_usd,
        -- Previous flow's destination protocol for this entity
        LAG(dest_protocol) OVER (
            PARTITION BY entity_address
            ORDER BY borrow_time, flow_id
        ) AS prev_dest_protocol,
        -- Previous flow's borrow_time for temporal constraint
        LAG(borrow_time) OVER (
            PARTITION BY entity_address
            ORDER BY borrow_time, flow_id
        ) AS prev_borrow_time
    FROM flows
),

-- Tag each flow: is it a continuation of the previous flow?
-- Continuation = source_protocol matches prev dest_protocol
-- AND within 1-hour temporal window (uniform for ALL hops)
flows_tagged AS (
    SELECT
        *,
        CASE
            WHEN prev_dest_protocol IS NOT NULL
                 AND source_protocol = prev_dest_protocol
                 AND borrow_time <= prev_borrow_time + INTERVAL '1' HOUR
            THEN 0  -- continuation of existing chain
            ELSE 1  -- start of a new chain
        END AS is_chain_start
    FROM flows_with_prev
),

-- Assign chain IDs using running sum of chain starts
-- (islands-and-gaps pattern: each new start increments the ID)
flows_with_chain AS (
    SELECT
        *,
        SUM(is_chain_start) OVER (
            PARTITION BY entity_address
            ORDER BY borrow_time, flow_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS chain_id
    FROM flows_tagged
),

-- ============================================================
-- AGGREGATE PER-CHAIN METRICS
-- ============================================================

chain_metrics AS (
    SELECT
        entity_address,
        chain_id,
        -- Root flow: first flow in each chain
        MIN_BY(flow_id, borrow_time) AS loop_id,
        MIN_BY(borrow_tx_hash, borrow_time) AS root_tx_hash,
        MIN(block_date) AS start_date,
        COUNT(*) AS hop_count,
        -- Build protocol path arrays for final concatenation
        ARRAY_AGG(source_protocol ORDER BY borrow_time) AS source_protocols,
        ARRAY_AGG(dest_protocol ORDER BY borrow_time) AS dest_protocols,
        SUM(COALESCE(amount_usd, 0)) AS gross_borrowed_usd
    FROM flows_with_chain
    GROUP BY entity_address, chain_id
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================

SELECT
    loop_id,
    entity_address,
    start_date,
    CAST(NULL AS DATE) AS end_date,
    -- Build protocols_involved: first source + all destinations
    ARRAY[source_protocols[1]] || dest_protocols AS protocols_involved,
    hop_count,
    hop_count AS recursion_depth,
    root_tx_hash,
    gross_borrowed_usd,
    -- Loop status based on depth
    CASE
        WHEN hop_count >= 3 THEN 'deep_loop'
        WHEN hop_count = 2 THEN 'standard_loop'
        ELSE 'single_hop'
    END AS loop_status
FROM chain_metrics
WHERE hop_count >= 1
