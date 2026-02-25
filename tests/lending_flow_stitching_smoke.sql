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
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

-- Resolve Morpho Blue stablecoin market IDs
morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
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
    FROM aave_v3_ethereum.pool_evt_borrow b
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
    FROM aave_v3_ethereum.pool_evt_supply s
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
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS asset_address  -- simplified
    FROM morpho_blue_ethereum.morphoblue_evt_borrow b
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
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS asset_address  -- simplified
    FROM morpho_blue_ethereum.morphoblue_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND s.id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Compound V3 borrows (Withdraw on USDC Comet, last 30 days)
compound_v3_borrows AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'compound_v3' AS protocol,
        w.src AS entity_address,
        w.contract_address AS asset_address
    FROM compound_v3_ethereum.comet_evt_withdraw w
    WHERE w.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND w.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

-- Compound V3 supplies (Supply on USDC Comet, last 30 days)
compound_v3_supplies AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'compound_v3' AS protocol,
        s.dst AS entity_address,
        s.contract_address AS asset_address
    FROM compound_v3_ethereum.comet_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND s.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

-- Compound V2 borrows (stablecoin cTokens, last 30 days)
compound_v2_borrows AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'compound_v2' AS protocol,
        b.borrower AS entity_address,
        b.contract_address AS asset_address
    FROM compound_ethereum.cerc20delegator_evt_borrow b
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND b.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

-- Compound V2 supplies (stablecoin cTokens, last 30 days)
compound_v2_supplies AS (
    SELECT
        m.evt_block_time AS block_time,
        m.evt_tx_hash AS tx_hash,
        m.evt_index,
        'compound_v2' AS protocol,
        m.minter AS entity_address,
        m.contract_address AS asset_address
    FROM compound_ethereum.cerc20delegator_evt_mint m
    WHERE m.evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND m.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

-- Union borrows and supplies across all 4 protocols
all_borrows AS (
    SELECT * FROM aave_borrows
    UNION ALL SELECT * FROM morpho_borrows
    UNION ALL SELECT * FROM compound_v3_borrows
    UNION ALL SELECT * FROM compound_v2_borrows
),
all_supplies AS (
    SELECT * FROM aave_supplies
    UNION ALL SELECT * FROM morpho_supplies
    UNION ALL SELECT * FROM compound_v3_supplies
    UNION ALL SELECT * FROM compound_v2_supplies
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
)

-- Results: count of same-tx cross-protocol flows by direction
SELECT
    'same_tx_flow_count' AS test_name,
    source_protocol,
    dest_protocol,
    COUNT(*) AS flow_count,
    COUNT(DISTINCT entity_address) AS unique_entities
FROM same_tx_flows
GROUP BY source_protocol, dest_protocol
ORDER BY flow_count DESC
