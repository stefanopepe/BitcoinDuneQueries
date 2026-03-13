WITH
stable_tokens AS (
  SELECT contract_address
  FROM tokens.erc20
  WHERE blockchain = 'base'
    AND lower(symbol) IN ('usdc', 'usdt', 'dai', 'usdbc')
),
btc_tokens AS (
  SELECT contract_address
  FROM tokens.erc20
  WHERE blockchain = 'base'
    AND lower(symbol) IN ('wbtc', 'cbbtc', 'tbtc')
),
morpho_markets_base AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token,
    from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
  FROM morpho_blue_base.morphoblue_evt_createmarket
),
borrows AS (
  SELECT
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    m.loan_token
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m
    ON m.market_id = b.id
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'
    AND b.evt_block_time < TIMESTAMP '{end_ts}'
    AND m.loan_token IN (SELECT contract_address FROM stable_tokens)
),
btc_supplies AS (
  SELECT
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_base.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_base m
    ON m.market_id = s.id
  JOIN btc_tokens bt
    ON bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '{start_ts}' - INTERVAL '1' DAY
    AND s.evt_block_time < TIMESTAMP '{end_ts}' + INTERVAL '1' DAY
),
cohort_borrows AS (
  SELECT b.*
  FROM borrows b
  WHERE EXISTS (
    SELECT 1 FROM btc_supplies pre
    WHERE pre.borrower = b.borrower
      AND pre.supply_time <= b.borrow_time
  )
),
loop_flags AS (
  SELECT
    b.borrow_tx_hash,
    EXISTS (
      SELECT 1 FROM btc_supplies s
      WHERE s.borrower = b.borrower
        AND s.supply_time >= b.borrow_time
        AND s.supply_time <= b.borrow_time + INTERVAL '24' HOUR
    ) AS loop24_proxy
  FROM cohort_borrows b
)
SELECT
  'base' AS chain,
  COUNT(*) AS n_borrows,
  COUNT_IF(loop24_proxy) AS n_loop24,
  CAST(COUNT_IF(loop24_proxy) AS DOUBLE) / NULLIF(COUNT(*), 0) AS ls24_event_rate
FROM loop_flags
