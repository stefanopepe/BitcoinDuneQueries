# Repository Queries Documentation

This document provides an overview of all SQL queries in the `/queries/` directory, including their purpose, input/output schemas, and dependencies.

> **Last Updated:** 2026-01-30

---

## Table of Contents

- [Overview](#overview)
- [Query Index](#query-index)
- [Query Dependencies](#query-dependencies)
- [Bitcoin Queries](#bitcoin-queries)
  - [bitcoin_utxo_heuristics.sql](#bitcoin_utxo_heuristicssql)
  - [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql)
  - [bitcoin_human_factor_scoring.sql](#bitcoin_human_factor_scoringsql)

---

## Overview

This repository organizes queries by blockchain:

```
queries/
├── bitcoin/           # Bitcoin network queries
├── ethereum/          # Ethereum mainnet queries (planned)
├── polygon/           # Polygon queries (planned)
├── arbitrum/          # Arbitrum queries (planned)
└── cross-chain/       # Multi-chain queries (planned)
```

### Query Naming Convention

- `{analysis_type}.sql` - General analysis queries
- `{protocol}_{analysis}.sql` - Protocol-specific queries
- `{metric}_daily.sql` - Daily aggregation queries

---

## Query Index

| Query | Blockchain | Dune Query ID | Description | Tables Used |
|-------|------------|---------------|-------------|-------------|
| [bitcoin_utxo_heuristics.sql](#bitcoin_utxo_heuristicssql) | Bitcoin | `query_6614095` | Classifies transactions by intent patterns | `bitcoin.inputs`, `bitcoin.outputs` |
| [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql) | Bitcoin | TBD | Detects privacy issues on "other" intent transactions | `bitcoin.inputs`, `bitcoin.outputs` |
| [bitcoin_human_factor_scoring.sql](#bitcoin_human_factor_scoringsql) | Bitcoin | TBD | Scores transactions on human vs automated origin | `bitcoin.inputs`, `bitcoin.outputs` |

---

## Query Dependencies

The following diagram shows the relationship between queries:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: UTXO HEURISTICS (query_6614095)                                   │
│  bitcoin_utxo_heuristics.sql                                                │
│                                                                             │
│  Classifies ALL transactions into intent categories:                        │
│  • consolidation, fan_out_batch, coinjoin_like, self_transfer              │
│  • change_like_2_outputs, malformed_no_outputs, other                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ "other" intent
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: PRIVACY HEURISTICS                                                │
│  bitcoin_privacy_heuristics_v2.sql                                          │
│                                                                             │
│  Analyzes ONLY "other" transactions for privacy patterns:                   │
│  • change_precision, change_script_type, uih1, uih2                        │
│  • address_reuse, no_privacy_issues                                         │
└─────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│  STANDALONE: HUMAN FACTOR SCORING                                           │
│  bitcoin_human_factor_scoring.sql                                           │
│                                                                             │
│  Scores ALL transactions on human vs automated likelihood (0-100):          │
│  • Uses BDD (Bitcoin Days Destroyed), tx structure, value patterns          │
│  • Outputs daily distribution by score bands                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Note:** The privacy heuristics query replicates the UTXO classification logic internally to filter to "other" transactions. It does not directly reference `query_6614095` since that query outputs aggregated data without transaction IDs.

**Note:** The human factor scoring query is standalone and processes all transactions independently.

---

## Bitcoin Queries

### bitcoin_utxo_heuristics.sql

**Path:** `queries/bitcoin/bitcoin_utxo_heuristics.sql`
**Dune Query ID:** `query_6614095`

**Description:**
Classifies Bitcoin transactions by their likely intent based on input/output patterns. Uses heuristics to identify consolidations, batch payments, CoinJoin-like transactions, and other common patterns.

**Author:** stefanopepe
**Created:** 2026-01-28

#### Purpose

Helps analysts understand the composition of Bitcoin transaction activity by categorizing transactions into behavioral intents rather than just raw counts.

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Source of transaction inputs | `block_time`, `tx_id`, `value`, `is_coinbase` |
| `bitcoin.outputs` | Transaction outputs | `block_time`, `tx_id`, `value` |

#### Input Parameters

This query uses Dune's incremental processing with `previous.query.result()`. No user-defined parameters required.

**Incremental Processing:**
- Uses 1-day lookback window for recomputation
- Default fallback date: `2026-01-01` (adjustable in `checkpoint` CTE)
- Excludes coinbase transactions from analysis

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `intent` | VARCHAR | Classified transaction intent category |
| `tx_count` | BIGINT | Number of transactions in category |
| `sats_in` | DOUBLE | Total input value (satoshis) |
| `sats_out` | DOUBLE | Total output value (satoshis) |
| `avg_inputs` | DOUBLE | Average input count per transaction |
| `avg_outputs` | DOUBLE | Average output count per transaction |
| `median_inputs` | DOUBLE | Median input count per transaction |
| `median_outputs` | DOUBLE | Median output count per transaction |

#### Intent Classification Logic

| Intent | Criteria | Description |
|--------|----------|-------------|
| `consolidation` | inputs ≥ 10, outputs ≤ 2 | Combining many UTXOs into few |
| `fan_out_batch` | inputs ≤ 2, outputs ≥ 10 | Batch payments (exchanges, payroll) |
| `coinjoin_like` | inputs ≥ 5, outputs ≥ 5, \|inputs - outputs\| ≤ 1 | Privacy-enhancing transactions |
| `self_transfer` | inputs = 1, outputs = 1 | Single input to single output |
| `change_like_2_outputs` | inputs ≥ 2, outputs = 2 | Standard payment with change |
| `malformed_no_outputs` | outputs = 0 | Edge case: no outputs |
| `other` | All other patterns | Unclassified transactions → analyzed by privacy heuristics |

#### Example Output

```
| day        | intent               | tx_count | sats_in        | sats_out       | avg_inputs | avg_outputs |
|------------|----------------------|----------|----------------|----------------|------------|-------------|
| 2026-01-27 | change_like_2_outputs| 245832   | 1234567890123  | 1234567890000  | 2.4        | 2.0         |
| 2026-01-27 | consolidation        | 12543    | 987654321098   | 987654321000   | 15.2       | 1.3         |
| 2026-01-27 | fan_out_batch        | 8921     | 567890123456   | 567890123400   | 1.5        | 45.7        |
| 2026-01-27 | coinjoin_like        | 1234     | 123456789012   | 123456789000   | 52.3       | 52.1        |
| 2026-01-27 | self_transfer        | 98765    | 345678901234   | 345678901200   | 1.0        | 1.0         |
| 2026-01-27 | other                | 156789   | 456789012345   | 456789012300   | 3.2        | 4.8         |
```

#### Notes

- **Coinbase Exclusion:** Coinbase transactions (block rewards) are excluded since they don't represent user-initiated activity
- **Incremental Design:** Query is designed for efficient incremental updates; only recomputes recent data
- **Thresholds:** The input/output count thresholds (10, 5, 2, 1) are configurable heuristics based on common Bitcoin usage patterns
- **Downstream:** The "other" category is further analyzed by `bitcoin_privacy_heuristics_v2.sql`

---

### bitcoin_privacy_heuristics_v2.sql

**Path:** `queries/bitcoin/bitcoin_privacy_heuristics_v2.sql`
**Dune Query ID:** TBD

**Description:**
Implements advanced privacy analysis heuristics based on Blockstream Esplora's privacy-analysis.js methodology. Analyzes ONLY transactions classified as "other" by the UTXO Heuristics query, providing deeper insight into this previously uncategorized bucket.

**Author:** stefanopepe
**Created:** 2026-01-29
**Updated:** 2026-01-29

**Reference:** [Blockstream Esplora privacy-analysis.js](https://github.com/Blockstream/esplora/blob/master/client/src/lib/privacy-analysis.js)

#### Purpose

Detects privacy-revealing patterns in Bitcoin transactions that weren't classified by the UTXO heuristics layer. Helps identify:
- Change output detection (via precision or script type)
- Unnecessary input usage patterns
- Address reuse vulnerabilities

#### Dependency

This query depends on the classification logic from `bitcoin_utxo_heuristics.sql` (`query_6614095`). It filters to only process transactions that would be classified as "other" by that query.

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Transaction inputs | `block_time`, `tx_id`, `index`, `value`, `address`, `type`, `is_coinbase` |
| `bitcoin.outputs` | Transaction outputs | `block_time`, `tx_id`, `index`, `value`, `address`, `type` |

#### Input Parameters

This query uses Dune's incremental processing with `previous.query.result()`. No user-defined parameters required.

**Incremental Processing:**
- Uses 1-day lookback window for recomputation
- Default fallback date: `2026-01-01` (adjustable in `checkpoint` CTE)
- Excludes coinbase transactions from analysis

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `privacy_heuristic` | VARCHAR | The privacy issue detected |
| `tx_count` | BIGINT | Number of transactions |
| `sats_total` | DOUBLE | Total satoshis involved |

#### Privacy Heuristics

| Heuristic | Criteria | Description |
|-----------|----------|-------------|
| `change_precision` | 2 outputs, ≥3 digit precision difference | Change detected via trailing zeros difference |
| `change_script_type` | 2 outputs, different script types, homogeneous inputs | Change detected via script type mismatch |
| `uih1` | inputs ≥2, outputs = 2, smallest input unnecessary for smallest output | Unnecessary Input Heuristic 1 |
| `uih2` | inputs ≥2, outputs = 2, smallest input unnecessary for largest output | Unnecessary Input Heuristic 2 |
| `address_reuse` | Output address matches input address | Privacy leak via address reuse |
| `no_privacy_issues` | None of above triggered | No detectable privacy issues |

**Note:** `coinjoin_detected` and `self_transfer` heuristics are handled by the UTXO Heuristics layer and are not duplicated here.

#### Example Output

```
| day        | privacy_heuristic   | tx_count | sats_total      |
|------------|---------------------|----------|-----------------|
| 2026-01-28 | no_privacy_issues   | 312456   | 8765432109876   |
| 2026-01-28 | change_precision    | 89234    | 2345678901234   |
| 2026-01-28 | self_transfer       | 45678    | 1234567890123   |
| 2026-01-28 | change_script_type  | 23456    | 567890123456    |
| 2026-01-28 | coinjoin_detected   | 1234     | 987654321098    |
| 2026-01-28 | address_reuse       | 5678     | 123456789012    |
| 2026-01-28 | uih1                | 4567     | 234567890123    |
| 2026-01-28 | uih2                | 1234     | 98765432109     |
```

#### Query Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: prev, checkpoint                                                      │
│   Incremental processing setup                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: raw_inputs, raw_outputs                                               │
│   Load raw transaction data for date range                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_counts, other_tx_ids                                               │
│   Apply UTXO classification → filter to "other" only                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_input_stats, tx_output_stats, two_output_details                   │
│   Aggregate transaction-level stats (filtered to "other")                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTE: address_reuse_detection                                                │
│   Detect address reuse patterns                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_combined, tx_with_precision                                        │
│   Join all data, calculate precision metrics                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTE: classified                                                             │
│   Apply privacy heuristic classification                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Final: UNION kept_old + new_data                                            │
│   Merge historical data with newly computed data                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Notes

- **Heuristic Priority:** Heuristics are evaluated in order; a transaction is classified by the first matching heuristic
- **Spendable Outputs Only:** Non-spendable outputs (OP_RETURN/nulldata, nonstandard) are excluded from analysis
- **UIH Exclusion:** UIH checks skip transactions where all inputs share the same script type (legitimate consolidation)
- **CoinJoin Threshold:** Requires ≥50% of outputs to be equal, capped between 2-5 matching outputs
- **Precision Calculation:** Uses trailing zeros in satoshi values to estimate "roundness"
- **Incremental Design:** Designed for efficient daily updates with 1-day lookback recomputation

---

### bitcoin_human_factor_scoring.sql

**Path:** `queries/bitcoin/bitcoin_human_factor_scoring.sql`
**Dune Query ID:** TBD

**Description:**
Scores Bitcoin transactions on their likelihood of originating from human-controlled wallets versus automated systems (exchanges, bots, mining pools). Uses behavioral heuristics including transaction structure, value patterns, and Bitcoin Days Destroyed (BDD) for holding time analysis.

**Author:** stefanopepe
**Created:** 2026-01-30
**Updated:** 2026-01-30

**Academic References:**
- Meiklejohn et al. (2013) - Clustering heuristics, entity tagging
- Ermilov et al. (2017) - Industrial-scale entity tagging
- Zhang et al. (2020) - Address reuse, clustering ratio
- Schnoering et al. (2024) - Temporal evolution, false positive analysis
- Niedermayer et al. (2024) - Bot detection taxonomy
- Sornette et al. (2024) - BDD holding time power-law distributions

#### Purpose

Enables analysts to:
- Filter likely human transactions for sentiment analysis
- Identify automated activity (exchange arbitrage, mining, mixing)
- Track changes in human vs. automated transaction volume over time
- Research behavioral patterns in Bitcoin usage

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Transaction inputs, BDD calculation | `block_time`, `tx_id`, `value`, `block_height`, `spent_block_height`, `is_coinbase` |
| `bitcoin.outputs` | Output features (dust, round values) | `block_time`, `tx_id`, `value` |

#### Input Parameters

This query uses Dune's incremental processing with `previous.query.result()`. No user-defined parameters required.

**Incremental Processing:**
- Uses 1-day lookback window for recomputation
- Default fallback date: `2026-01-01` (adjustable in `checkpoint` CTE)
- Excludes coinbase transactions from analysis

#### Scoring Model

**BASE_SCORE = 50** (neutral starting point)

| Indicator | Condition | Weight | Direction |
|-----------|-----------|--------|-----------|
| `high_fan_in` | input_count > 50 | -15 | Automated |
| `high_fan_out` | output_count > 50 | -15 | Automated |
| `round_values` | output divisible by 0.001 BTC | -5 | Automated |
| `dust_output` | any output < 0.00000546 BTC | -10 | Automated |
| `simple_structure` | 1-in-1-out or 1-in-2-out | +10 | Human |
| `non_round_value` | no round outputs | +5 | Human |
| `moderate_holder` | avg days held 1-365 | +10 | Human |
| `long_term_holder` | avg days held > 365 | +15 | Human |

Final score clamped to [0, 100].

**Note:** BDD calculation uses `spent_block_height` from `bitcoin.inputs` with approximation: `days_held = (block_height - spent_block_height) / 144`

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `score_band` | VARCHAR | Score range (e.g., '50-60') |
| `score_band_order` | BIGINT | Numeric ordering (1-10) |
| `tx_count` | BIGINT | Number of transactions in band |
| `btc_volume` | DOUBLE | Total BTC moved (input value) |
| `avg_score` | DOUBLE | Average exact score within band |

#### Score Band Interpretation

| Band | Range | Interpretation |
|------|-------|----------------|
| 1-3 | 0-30 | Likely automated (exchange, pool, bot) |
| 4-5 | 30-50 | Probably automated |
| 6 | 50-60 | Ambiguous / uncertain |
| 7-8 | 60-80 | Likely human-controlled |
| 9-10 | 80-100 | Strong human indicators (HODLer) |

#### Example Output

```
| day        | score_band | score_band_order | tx_count | btc_volume    | avg_score |
|------------|------------|------------------|----------|---------------|-----------|
| 2026-01-29 | 50-60      | 6                | 125000   | 45000.123     | 55.2      |
| 2026-01-29 | 60-70      | 7                | 98000    | 32000.456     | 65.8      |
| 2026-01-29 | 70-80      | 8                | 45000    | 18000.789     | 75.3      |
| 2026-01-29 | 40-50      | 5                | 35000    | 12000.012     | 45.1      |
| 2026-01-29 | 30-40      | 4                | 22000    | 8500.345      | 35.6      |
```

#### Query Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: prev, checkpoint                                                       │
│   Incremental processing setup                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: raw_inputs, raw_outputs                                                │
│   Load raw transaction data for date range                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_input_stats, tx_output_stats                                        │
│   Aggregate input and output features per transaction                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTE: tx_combined                                                             │
│   Join all features, derive boolean indicators                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_scored, tx_with_bands                                               │
│   Apply scoring formula, assign to score bands                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ CTE: new_data                                                                │
│   Aggregate by day and score band                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Final: UNION kept_old + new_data                                             │
│   Merge historical data with newly computed data                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Notes

- **Academic Foundation:** Based on peer-reviewed research on blockchain entity classification
- **BDD Calculation:** Uses `spent_block_height` to calculate holding time without expensive joins
- **No Definitive Proof:** Score indicates *likelihood*, not certainty (per Schnoering et al., 2024)
- **Incremental Design:** Designed for efficient daily updates with 1-day lookback recomputation
- **Coinbase Exclusion:** Coinbase transactions (block rewards) are excluded since they don't represent user-initiated activity
- **Value Units:** `bitcoin.inputs.value` and `bitcoin.outputs.value` are in BTC (not satoshis)
- **Future Enhancement:** Label integration (exchange/mining/mixer tags) can be added when `labels.addresses` has Bitcoin coverage

#### Limitations

1. No method definitively proves human control - score indicates likelihood only
2. Privacy-preserving wallets (CoinJoin users) may score lower despite being human
3. Sophisticated bots can mimic human patterns and defeat heuristics
4. BDD approximation uses 144 blocks = 1 day (actual block times vary)

---

## Adding New Queries

When adding a new query to this repository:

1. **Create the SQL file** in the appropriate blockchain directory
2. **Include the standard header** (see [CLAUDE.md](../CLAUDE.md) for format)
3. **Validate against schemas** in [`dune_database_schemas.md`](./dune_database_schemas.md)
4. **Document in this file** following the template below

### Query Documentation Template

```markdown
### query_name.sql

**Path:** `queries/{blockchain}/query_name.sql`

**Description:**
[Brief description of what the query does]

**Author:** [username]
**Created:** [YYYY-MM-DD]

#### Purpose
[Why this query is useful]

#### Dune Tables Used
| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `table.name` | [purpose] | `col1`, `col2` |

#### Input Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `{{param}}` | TYPE | value | [description] |

#### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| `column_name` | TYPE | [description] |

#### Notes
[Any special considerations, limitations, or usage notes]
```

---

## See Also

- [README.md](./README.md) - Contributing guidelines and query documentation template
- [CLAUDE.md](../CLAUDE.md) - Development guidelines and query conventions
- [dune_database_schemas.md](./dune_database_schemas.md) - Dune table schemas reference
