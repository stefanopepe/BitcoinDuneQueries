-- ============================================================
-- Query: Lending Loop Detection (Nested Query)
-- Description: Detects multi-hop lending loops where entities:
--              1. Supply collateral to P1
--              2. Borrow stablecoin from P1
--              3. Supply to P2
--              4. Borrow again from P2
--              5. (optionally continue...)
--              Uses the flow stitching query to trace loop paths.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-05
-- Architecture: V2 Nested Query (builds on flow_stitching)
-- Dependencies: lending_flow_stitching
-- ============================================================
-- Output Columns:
--   loop_id              - Unique loop identifier
--   entity_address       - Entity executing the loop
--   start_date           - Loop initiation date
--   end_date             - Loop end date (if closed)
--   protocols_involved   - Array of protocols in loop path
--   hop_count            - Number of protocol hops
--   recursion_depth      - Max depth of recursive borrowing
--   root_tx_hash         - First transaction in loop
--   gross_borrowed_usd   - Total USD borrowed across all hops
--   loop_status          - active/closed/partial
-- ============================================================

WITH
-- Reference the flow stitching query (column-pruned)
flows AS (
    SELECT
        flow_id,
        block_date,
        entity_address,
        source_protocol,
        dest_protocol,
        asset_address,
        borrow_tx_hash,
        borrow_time,
        amount_usd
    FROM query_6690272
),

-- ============================================================
-- IDENTIFY LOOP CHAINS
-- A loop is a sequence of flows where the destination protocol
-- of one flow becomes the source of the next
-- ============================================================

-- First hop: initial cross-protocol flow
first_hops AS (
    SELECT
        flow_id AS root_flow_id,
        entity_address,
        block_date AS start_date,
        source_protocol AS p1,
        dest_protocol AS p2,
        asset_address,
        borrow_tx_hash AS root_tx_hash,
        borrow_time,
        COALESCE(amount_usd, 0) AS hop_amount_usd,
        1 AS hop_number,
        ARRAY[source_protocol, dest_protocol] AS protocol_path
    FROM flows
),

-- Second hop: flow FROM the first destination protocol
second_hops AS (
    SELECT
        f1.root_flow_id,
        f1.entity_address,
        f1.start_date,
        f1.p1,
        f2.dest_protocol AS p3,
        f1.root_tx_hash,
        f1.hop_amount_usd + COALESCE(f2.amount_usd, 0) AS cumulative_amount_usd,
        2 AS hop_number,
        f1.protocol_path || f2.dest_protocol AS protocol_path
    FROM first_hops f1
    INNER JOIN flows f2
        ON f2.entity_address = f1.entity_address
        AND f2.source_protocol = f1.p2  -- Continue from first destination
        AND f2.borrow_time > f1.borrow_time
        AND f2.borrow_time <= f1.borrow_time + INTERVAL '1' HOUR  -- Within 1 hour
),

-- Third hop (if exists)
third_hops AS (
    SELECT
        f2.root_flow_id,
        f2.entity_address,
        f2.start_date,
        f2.p1,
        f3.dest_protocol AS p4,
        f2.root_tx_hash,
        f2.cumulative_amount_usd + COALESCE(f3.amount_usd, 0) AS cumulative_amount_usd,
        3 AS hop_number,
        f2.protocol_path || f3.dest_protocol AS protocol_path
    FROM second_hops f2
    INNER JOIN flows f3
        ON f3.entity_address = f2.entity_address
        AND f3.source_protocol = f2.p3
        AND f3.block_date >= f2.start_date
),

-- ============================================================
-- AGGREGATE LOOP METRICS
-- ============================================================

-- Combine all hops and find max depth per entity/root
loop_paths AS (
    SELECT root_flow_id, entity_address, start_date, root_tx_hash,
           hop_amount_usd AS gross_borrowed_usd, hop_number, protocol_path
    FROM first_hops
    UNION ALL
    SELECT root_flow_id, entity_address, start_date, root_tx_hash,
           cumulative_amount_usd, hop_number, protocol_path
    FROM second_hops
    UNION ALL
    SELECT root_flow_id, entity_address, start_date, root_tx_hash,
           cumulative_amount_usd, hop_number, protocol_path
    FROM third_hops
),

-- Get the deepest path for each loop
max_depth_loops AS (
    SELECT
        root_flow_id AS loop_id,
        entity_address,
        start_date,
        root_tx_hash,
        MAX(hop_number) AS recursion_depth,
        MAX_BY(protocol_path, hop_number) AS protocols_involved,
        MAX(gross_borrowed_usd) AS gross_borrowed_usd
    FROM loop_paths
    GROUP BY
        root_flow_id,
        entity_address,
        start_date,
        root_tx_hash
)

SELECT
    loop_id,
    entity_address,
    start_date,
    CAST(NULL AS DATE) AS end_date,  -- Would need repay tracking
    protocols_involved,
    CARDINALITY(protocols_involved) - 1 AS hop_count,
    recursion_depth,
    root_tx_hash,
    gross_borrowed_usd,
    -- Loop status based on depth
    CASE
        WHEN recursion_depth >= 3 THEN 'deep_loop'
        WHEN recursion_depth = 2 THEN 'standard_loop'
        ELSE 'single_hop'
    END AS loop_status
FROM max_depth_loops
WHERE recursion_depth >= 1  -- At least one cross-protocol hop
