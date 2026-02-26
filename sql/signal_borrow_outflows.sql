WITH
stable_tokens AS (
  SELECT blockchain, contract_address
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum', 'base')
    AND lower(symbol) IN ('usdc', 'usdt', 'dai')
),
morpho_markets_eth AS (
  SELECT id AS market_id, from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
  FROM morpho_blue_ethereum.morphoblue_evt_createmarket
),
morpho_markets_base AS (
  SELECT id AS market_id, from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
  FROM morpho_blue_base.morphoblue_evt_createmarket
),
base_borrows AS (
  SELECT
    'ethereum' AS chain,
    b.evt_block_time AS borrow_time,
    b.evt_tx_hash AS borrow_tx_hash,
    lower(to_hex(COALESCE(b.onBehalf, b.caller))) AS receiver,
    m.loan_token AS stable_token
  FROM morpho_blue_ethereum.morphoblue_evt_borrow b
  JOIN morpho_markets_eth m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'ethereum' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'

  UNION ALL

  SELECT
    'base' AS chain,
    b.evt_block_time AS borrow_time,
    b.evt_tx_hash AS borrow_tx_hash,
    lower(to_hex(COALESCE(b.onBehalf, b.caller))) AS receiver,
    m.loan_token AS stable_token
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'base' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'
),
raw_outflows AS (
  SELECT
    b.chain,
    b.borrow_tx_hash,
    b.receiver,
    lower(to_hex(t."to")) AS recipient,
    COALESCE(t.amount_usd, 0.0) AS transfer_amount_usd,
    row_number() OVER (PARTITION BY b.borrow_tx_hash ORDER BY t.amount_usd DESC NULLS LAST) AS rn
  FROM base_borrows b
  JOIN tokens.transfers t
    ON t.blockchain = b.chain
   AND lower(to_hex(t."from")) = b.receiver
   AND t.contract_address = b.stable_token
   AND t.block_time >= b.borrow_time
   AND t.block_time <= b.borrow_time + INTERVAL '7' DAY
),
recipients AS (
  SELECT DISTINCT chain, recipient
  FROM raw_outflows
),
contracts AS (
  SELECT 'ethereum' AS chain, lower(to_hex(address)) AS addr
  FROM ethereum.creation_traces
  WHERE lower(to_hex(address)) IN (SELECT recipient FROM recipients WHERE chain = 'ethereum')
  UNION ALL
  SELECT 'base' AS chain, lower(to_hex(address)) AS addr
  FROM base.creation_traces
  WHERE lower(to_hex(address)) IN (SELECT recipient FROM recipients WHERE chain = 'base')
),
outflows AS (
  SELECT
    r.chain,
    r.borrow_tx_hash,
    r.receiver,
    r.recipient,
    CASE WHEN c.addr IS NULL THEN 'eoa' ELSE 'contract' END AS recipient_type,
    r.transfer_amount_usd,
    r.rn
  FROM raw_outflows r
  LEFT JOIN contracts c
    ON c.chain = r.chain
   AND c.addr = r.recipient
)
SELECT chain, borrow_tx_hash, receiver, recipient, recipient_type, transfer_amount_usd
FROM outflows
WHERE rn <= 5
