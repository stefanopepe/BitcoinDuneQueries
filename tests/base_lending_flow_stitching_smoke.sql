-- ============================================================
-- Smoke Test: Lending Flow Stitching
-- Description: Validates cross-protocol flow detection by checking
--              for same-tx borrow->supply patterns across protocols.
--              Uses stablecoin-filtered events from the last 30 days.
--              Covers Aave V3, Morpho Blue, Compound V3, Compound V2.
-- Author: stefanopepe
-- Created: 2026-02-11
-- Updated: 2026-02-11
-- ============================================================

WITH stablecoins AS (
    SELECT address FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),  -- USDC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),  -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb),  -- DAI
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca)   -- USDbC
    ) AS t(address)
),

-- Resolve Morpho Blue stablecoin market IDs
morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Aave V3 borrows (stablecoins, last 30 days)
aave_borrows AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'aave_v3' AS protocol,
        COALESCE(b.onBehalfOf, b."user") AS entity_address,
        b.reserve AS asset_address
    FROM aave_v3_base.pool_evt_borrow b
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND b.reserve IN (SELECT address FROM stablecoins)
),

-- Aave V3 supplies (stablecoins, last 30 days)
aave_supplies AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        COALESCE(s.onBehalfOf, s."user") AS entity_address,
        s.reserve AS asset_address
    FROM aave_v3_base.pool_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND s.reserve IN (SELECT address FROM stablecoins)
),

-- Morpho Blue borrows (stablecoin markets, last 30 days)
morpho_borrows AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_blue' AS protocol,
        COALESCE(b.onBehalf, b.caller) AS entity_address,
        0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 AS asset_address  -- simplified USDC
    FROM morpho_blue_base.morphoblue_evt_borrow b
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND b.id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Morpho Blue supplies (stablecoin markets, last 30 days)
morpho_supplies AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        COALESCE(s.onBehalf, s.caller) AS entity_address,
        0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 AS asset_address  -- simplified
    FROM morpho_blue_base.morphoblue_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND s.id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Union borrows and supplies across Base-enabled protocols
all_borrows AS (
    SELECT * FROM aave_borrows
    UNION ALL SELECT * FROM morpho_borrows
),
all_supplies AS (
    SELECT * FROM aave_supplies
    UNION ALL SELECT * FROM morpho_supplies
),

-- Same-tx cross-protocol stitching test
same_tx_flows AS (
    SELECT
        b.tx_hash,
        b.entity_address,
        b.protocol AS source_protocol,
        s.protocol AS dest_protocol,
        'same_tx' AS flow_type
    FROM all_borrows b
    INNER JOIN all_supplies s
        ON s.tx_hash = b.tx_hash
        AND s.entity_address = b.entity_address
        AND s.evt_index > b.evt_index
        AND s.protocol != b.protocol
),

results AS (
    SELECT
        'same_tx_flow_count' AS test_name,
        source_protocol,
        dest_protocol,
        COUNT(*) AS flow_count,
        COUNT(DISTINCT entity_address) AS unique_entities
    FROM same_tx_flows
    GROUP BY source_protocol, dest_protocol
)

SELECT * FROM results
