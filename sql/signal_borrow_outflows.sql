WITH
stable_tokens AS (
  SELECT contract_address
  FROM tokens.erc20
  WHERE blockchain = 'base'
    AND lower(symbol) IN ('usdc', 'usdt', 'dai', 'usdbc')
),
morpho_markets_base AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
  FROM morpho_blue_base.morphoblue_evt_createmarket
),
base_borrows AS (
  SELECT
    b.evt_block_time AS borrow_time,
    b.evt_tx_hash AS borrow_tx_hash,
    lower(to_hex(COALESCE(b.onBehalf, b.caller))) AS receiver,
    m.loan_token AS stable_token
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m
    ON m.market_id = b.id
  JOIN stable_tokens st
    ON st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'
    AND b.evt_block_time < TIMESTAMP '{end_ts}'
),
raw_outflows AS (
  SELECT
    b.borrow_tx_hash,
    b.receiver,
    lower(to_hex(t."to")) AS recipient,
    COALESCE(t.amount_usd, 0.0) AS transfer_amount_usd,
    row_number() OVER (
      PARTITION BY b.borrow_tx_hash
      ORDER BY t.amount_usd DESC NULLS LAST
    ) AS rn
  FROM base_borrows b
  JOIN tokens.transfers t
    ON t.blockchain = 'base'
   AND t.block_time >= TIMESTAMP '{start_ts}'
   AND t.block_time < TIMESTAMP '{end_ts}' + INTERVAL '7' DAY
   AND lower(to_hex(t."from")) = b.receiver
   AND t.contract_address = b.stable_token
   AND t.block_time >= b.borrow_time
   AND t.block_time <= b.borrow_time + INTERVAL '7' DAY
),
recipients AS (
  SELECT DISTINCT recipient
  FROM raw_outflows
),
base_contracts AS (
  SELECT lower(to_hex(address)) AS addr
  FROM base.creation_traces
  WHERE lower(to_hex(address)) IN (SELECT recipient FROM recipients)
),
outflows AS (
  SELECT
    r.borrow_tx_hash,
    r.receiver,
    r.recipient,
    CASE WHEN c.addr IS NULL THEN 'eoa' ELSE 'contract' END AS recipient_type,
    r.transfer_amount_usd,
    r.rn
  FROM raw_outflows r
  LEFT JOIN base_contracts c
    ON c.addr = r.recipient
)
SELECT
  'base' AS chain,
  borrow_tx_hash,
  receiver,
  recipient,
  recipient_type,
  transfer_amount_usd
FROM outflows
WHERE rn <= 5
