-- ============================================================
-- Query: Lending Action Ledger - Morpho Blue (Base Query)
-- Description: Unified action ledger for Morpho Blue lending events on Base.
--              Fetches Supply, Borrow, Repay, Withdraw, and Liquidation
--              and normalizes them into a single schema for downstream analysis.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-18
-- Architecture: V2 Base Query - computes ALL actions once
-- Dependencies: None (base query)
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier ('morpho_blue')
--   action_type          - Action: supply/borrow/repay/withdraw/liquidation
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address
--   asset_address        - Underlying loan asset contract
--   market_id            - Morpho market ID
--   amount_raw           - Raw amount in asset decimals
--   amount               - Decimal-adjusted amount
--   amount_usd           - USD value at event time
-- ============================================================

WITH
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            block_time TIMESTAMP,
            block_date DATE,
            block_number BIGINT,
            tx_hash VARBINARY,
            evt_index BIGINT,
            protocol VARCHAR,
            action_type VARCHAR,
            user_address VARBINARY,
            on_behalf_of VARBINARY,
            asset_address VARBINARY,
            market_id VARCHAR,
            amount_raw VARCHAR,
            amount DOUBLE,
            amount_usd DOUBLE
        )
    ))
),

checkpoint AS (
    SELECT
        COALESCE(MAX(block_date), DATE '2024-01-01') - INTERVAL '1' DAY AS cutoff_date
    FROM prev
),

stablecoin_metadata AS (
    SELECT address, symbol, decimals
    FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC',   6),
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 'USDbC',  6),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'USDT',   6),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI',   18)
    ) AS t(address, symbol, decimals)
),

stablecoins AS (
    SELECT address FROM stablecoin_metadata
),

morpho_blue_stablecoin_markets AS (
    SELECT
        id AS market_id,
        from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

supply_events AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        'supply' AS action_type,
        s.caller AS user_address,
        s.onBehalf AS on_behalf_of,
        m.loan_token AS asset_address,
        CAST(s.id AS VARCHAR) AS market_id,
        CAST(s.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_supply s
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = s.id
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
),

borrow_events AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_block_date AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_blue' AS protocol,
        'borrow' AS action_type,
        b.caller AS user_address,
        b.onBehalf AS on_behalf_of,
        m.loan_token AS asset_address,
        CAST(b.id AS VARCHAR) AS market_id,
        CAST(b.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_borrow b
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = b.id
    CROSS JOIN checkpoint c
    WHERE b.evt_block_date >= c.cutoff_date
      AND b.evt_block_date < CURRENT_DATE
),

repay_events AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'morpho_blue' AS protocol,
        'repay' AS action_type,
        r.caller AS user_address,
        r.onBehalf AS on_behalf_of,
        m.loan_token AS asset_address,
        CAST(r.id AS VARCHAR) AS market_id,
        CAST(r.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_repay r
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = r.id
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
),

withdraw_events AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_blue' AS protocol,
        'withdraw' AS action_type,
        w.caller AS user_address,
        w.onBehalf AS on_behalf_of,
        m.loan_token AS asset_address,
        CAST(w.id AS VARCHAR) AS market_id,
        CAST(w.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_withdraw w
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = w.id
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
),

liquidation_events AS (
    SELECT
        l.evt_block_time AS block_time,
        l.evt_block_date AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'morpho_blue' AS protocol,
        'liquidation' AS action_type,
        l.caller AS user_address,
        l.borrower AS on_behalf_of,
        m.loan_token AS asset_address,
        CAST(l.id AS VARCHAR) AS market_id,
        CAST(l.repaidAssets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_liquidate l
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = l.id
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
),

all_events AS (
    SELECT * FROM supply_events
    UNION ALL
    SELECT * FROM borrow_events
    UNION ALL
    SELECT * FROM repay_events
    UNION ALL
    SELECT * FROM withdraw_events
    UNION ALL
    SELECT * FROM liquidation_events
),

enriched AS (
    SELECT
        e.block_time,
        e.block_date,
        e.block_number,
        e.tx_hash,
        e.evt_index,
        e.protocol,
        e.action_type,
        e.user_address,
        e.on_behalf_of,
        e.asset_address,
        e.market_id,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, sm.decimals) AS amount,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, sm.decimals) * p.price AS amount_usd
    FROM all_events e
    LEFT JOIN stablecoin_metadata sm
        ON sm.address = e.asset_address
    LEFT JOIN prices.usd p
        ON p.contract_address = e.asset_address
        AND p.blockchain = 'base'
        AND p.minute = date_trunc('minute', e.block_time)
        AND p.minute >= (SELECT cutoff_date FROM checkpoint)
        AND p.minute < CURRENT_DATE
),

new_data AS (
    SELECT * FROM enriched
),

kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.block_date < c.cutoff_date
)

SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
