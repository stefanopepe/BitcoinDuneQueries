WITH
stable_tokens AS (
  SELECT blockchain, contract_address, lower(symbol) AS symbol, decimals
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum', 'base')
    AND lower(symbol) IN ('usdc', 'usdt', 'dai')
),
btc_tokens AS (
  SELECT blockchain, contract_address
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum', 'base')
    AND lower(symbol) IN ('wbtc', 'cbbtc', 'tbtc')
),
morpho_markets_eth AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token,
    from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
  FROM morpho_blue_ethereum.morphoblue_evt_createmarket
),
morpho_markets_base AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token,
    from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
  FROM morpho_blue_base.morphoblue_evt_createmarket
),
all_stable_borrows AS (
  SELECT
    'ethereum' AS chain,
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    m.loan_token AS stable_token,
    CAST(b.assets AS DOUBLE) AS amount_raw
  FROM morpho_blue_ethereum.morphoblue_evt_borrow b
  JOIN morpho_markets_eth m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'ethereum' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'

  UNION ALL

  SELECT
    'base' AS chain,
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    m.loan_token AS stable_token,
    CAST(b.assets AS DOUBLE) AS amount_raw
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'base' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'
),
btc_collateral_supplies AS (
  SELECT
    'ethereum' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_eth m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='ethereum' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '{start_ts}' - INTERVAL '30' DAY

  UNION ALL

  SELECT
    'base' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_base.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_base m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='base' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '{start_ts}' - INTERVAL '30' DAY
),
btc_backed_borrows AS (
  SELECT b.*
  FROM all_stable_borrows b
  WHERE EXISTS (
    SELECT 1 FROM btc_collateral_supplies s
    WHERE s.chain = b.chain
      AND s.borrower = b.borrower
      AND s.supply_time <= b.borrow_time
  )
),
all_enriched AS (
  SELECT
    b.chain,
    b.borrow_tx_hash,
    lower(to_hex(b.borrower)) AS borrower,
    b.amount_raw / POWER(10, st.decimals) AS borrow_usd_approx
  FROM all_stable_borrows b
  JOIN stable_tokens st ON st.blockchain = b.chain AND st.contract_address = b.stable_token
),
cohort_enriched AS (
  SELECT
    b.chain,
    b.borrow_tx_hash,
    lower(to_hex(b.borrower)) AS borrower,
    b.amount_raw / POWER(10, st.decimals) AS borrow_usd_approx
  FROM btc_backed_borrows b
  JOIN stable_tokens st ON st.blockchain = b.chain AND st.contract_address = b.stable_token
),
agg_all AS (
  SELECT
    chain,
    COUNT(*) AS all_borrow_events,
    COUNT(DISTINCT borrower) AS all_borrowers,
    SUM(borrow_usd_approx) AS all_borrow_usd_approx
  FROM all_enriched
  GROUP BY 1
),
agg_cohort AS (
  SELECT
    chain,
    COUNT(*) AS cohort_borrow_events,
    COUNT(DISTINCT borrower) AS cohort_borrowers,
    SUM(borrow_usd_approx) AS cohort_borrow_usd_approx
  FROM cohort_enriched
  GROUP BY 1
)
SELECT
  a.chain,
  a.all_borrow_events,
  COALESCE(c.cohort_borrow_events, 0) AS cohort_borrow_events,
  CASE WHEN a.all_borrow_events > 0 THEN COALESCE(c.cohort_borrow_events, 0) / CAST(a.all_borrow_events AS DOUBLE) ELSE 0 END AS event_coverage_share,
  a.all_borrowers,
  COALESCE(c.cohort_borrowers, 0) AS cohort_borrowers,
  CASE WHEN a.all_borrowers > 0 THEN COALESCE(c.cohort_borrowers, 0) / CAST(a.all_borrowers AS DOUBLE) ELSE 0 END AS borrower_coverage_share,
  a.all_borrow_usd_approx,
  COALESCE(c.cohort_borrow_usd_approx, 0) AS cohort_borrow_usd_approx,
  CASE WHEN a.all_borrow_usd_approx > 0 THEN COALESCE(c.cohort_borrow_usd_approx, 0) / a.all_borrow_usd_approx ELSE 0 END AS volume_coverage_share
FROM agg_all a
LEFT JOIN agg_cohort c
  ON a.chain = c.chain
ORDER BY 1
