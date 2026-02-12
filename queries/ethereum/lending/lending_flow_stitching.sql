-- ============================================================
-- Query: Lending Flow Stitching (Nested Query)
-- Description: Detects cross-protocol capital flows by stitching:
--              1. Borrow events on Protocol P1
--              2. ERC-20 transfers of borrowed asset
--              3. Supply events on Protocol P2
--              Uses a 10-block (~2 minute) time window for cross-tx flows.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-05
-- Architecture: V2 Nested Query (joins base with transfers.erc20)
-- Dependencies: lending_action_ledger_unified
-- ============================================================
-- Output Columns:
--   flow_id              - Unique flow identifier
--   block_date           - Flow date
--   entity_address       - Entity executing the flow
--   source_protocol      - Protocol where borrow occurred
--   dest_protocol        - Protocol where supply occurred
--   asset_address        - Asset being moved
--   asset_symbol         - Token symbol
--   borrow_tx_hash       - Borrow transaction
--   supply_tx_hash       - Supply transaction
--   borrow_time          - Borrow timestamp
--   supply_time          - Supply timestamp
--   time_delta_seconds   - Time between borrow and supply
--   is_same_tx           - Whether flow occurred in same transaction
--   amount               - Flow amount (from borrow)
--   amount_usd           - USD value of flow
-- ============================================================

WITH
-- Reference the unified action ledger base query (column-pruned)
base_actions AS (
    SELECT
        block_time,
        block_date,
        block_number,
        tx_hash,
        evt_index,
        protocol,
        action_type,
        entity_address,
        asset_address,
        asset_symbol,
        amount,
        amount_usd
    FROM query_6687961
),

-- Extract borrow events (source of cross-protocol flows)
borrows AS (
    SELECT
        block_time,
        block_date,
        block_number,
        tx_hash,
        evt_index,
        protocol AS source_protocol,
        entity_address,
        asset_address,
        asset_symbol,
        amount,
        amount_usd
    FROM base_actions
    WHERE action_type = 'borrow'
      AND entity_address IS NOT NULL
),

-- Extract supply events (destination of cross-protocol flows)
supplies AS (
    SELECT
        block_time,
        block_date,
        block_number,
        tx_hash,
        evt_index,
        protocol AS dest_protocol,
        entity_address,
        asset_address,
        asset_symbol,
        amount,
        amount_usd
    FROM base_actions
    WHERE action_type = 'supply'
      AND entity_address IS NOT NULL
),

-- ============================================================
-- SAME-TRANSACTION FLOWS
-- Borrow and supply in same tx (atomic/flash loan style)
-- ============================================================

same_tx_flows AS (
    SELECT
        CONCAT(
            CAST(b.tx_hash AS VARCHAR), '-',
            CAST(b.evt_index AS VARCHAR), '-',
            CAST(s.evt_index AS VARCHAR)
        ) AS flow_id,
        b.block_date,
        b.entity_address,
        b.source_protocol,
        s.dest_protocol,
        b.asset_address,
        b.asset_symbol,
        b.tx_hash AS borrow_tx_hash,
        s.tx_hash AS supply_tx_hash,
        b.block_time AS borrow_time,
        s.block_time AS supply_time,
        0 AS time_delta_seconds,
        TRUE AS is_same_tx,
        b.amount,
        b.amount_usd
    FROM borrows b
    INNER JOIN supplies s
        ON s.tx_hash = b.tx_hash
        AND s.entity_address = b.entity_address
        AND s.asset_address = b.asset_address
        AND s.evt_index > b.evt_index  -- Supply after borrow in same tx
        AND s.dest_protocol != b.source_protocol  -- Cross-protocol
),

-- ============================================================
-- CROSS-TRANSACTION FLOWS (within 10 blocks / ~2 minutes)
-- Borrow on P1, then supply on P2 in different transaction
-- ============================================================

cross_tx_ranked AS (
    SELECT
        CONCAT(
            CAST(b.tx_hash AS VARCHAR), '-',
            CAST(s.tx_hash AS VARCHAR)
        ) AS flow_id,
        b.block_date,
        b.entity_address,
        b.source_protocol,
        s.dest_protocol,
        b.asset_address,
        b.asset_symbol,
        b.tx_hash AS borrow_tx_hash,
        s.tx_hash AS supply_tx_hash,
        b.block_time AS borrow_time,
        s.block_time AS supply_time,
        CAST(
            date_diff('second', b.block_time, s.block_time) AS INTEGER
        ) AS time_delta_seconds,
        FALSE AS is_same_tx,
        b.amount,
        b.amount_usd,
        -- Deduplicate: keep only the first supply after each borrow
        ROW_NUMBER() OVER (
            PARTITION BY b.tx_hash, b.evt_index
            ORDER BY s.block_time
        ) AS rn
    FROM borrows b
    INNER JOIN supplies s
        ON s.entity_address = b.entity_address
        AND s.asset_address = b.asset_address
        AND s.tx_hash != b.tx_hash  -- Different transaction
        AND s.dest_protocol != b.source_protocol  -- Cross-protocol
        -- Time window: supply within 2 minutes after borrow
        AND s.block_time > b.block_time
        AND s.block_time <= b.block_time + INTERVAL '2' MINUTE
),

cross_tx_flows AS (
    SELECT flow_id, block_date, entity_address, source_protocol, dest_protocol,
           asset_address, asset_symbol, borrow_tx_hash, supply_tx_hash,
           borrow_time, supply_time, time_delta_seconds, is_same_tx,
           amount, amount_usd
    FROM cross_tx_ranked
    WHERE rn = 1
),

-- ============================================================
-- COMBINE ALL FLOWS
-- ============================================================

all_flows AS (
    SELECT * FROM same_tx_flows
    UNION ALL
    SELECT * FROM cross_tx_flows
)

SELECT
    flow_id,
    block_date,
    entity_address,
    source_protocol,
    dest_protocol,
    asset_address,
    asset_symbol,
    borrow_tx_hash,
    supply_tx_hash,
    borrow_time,
    supply_time,
    time_delta_seconds,
    is_same_tx,
    amount,
    amount_usd,
    -- Flow classification
    CASE
        WHEN is_same_tx THEN 'atomic'
        WHEN time_delta_seconds <= 15 THEN 'near_instant'
        WHEN time_delta_seconds <= 60 THEN 'fast'
        ELSE 'delayed'
    END AS flow_speed_category
FROM all_flows
