# Repository Queries Documentation

This document provides an overview of all SQL queries in the `/queries/` directory, including their purpose, input/output schemas, and dependencies.

> **Last Updated:** 2026-02-10

---

## Table of Contents

- [Overview](#overview)
- [Query Index](#query-index)
- [Query Dependencies](#query-dependencies)
- [Unified Query Architecture (V2)](#unified-query-architecture-v2)
  - [bitcoin_tx_features_daily.sql](#bitcoin_tx_features_dailysql) (Base Query)
  - [bitcoin_human_factor_scoring_v2.sql](#bitcoin_human_factor_scoring_v2sql)
  - [bitcoin_human_factor_cohort_matrix.sql](#bitcoin_human_factor_cohort_matrixsql)
  - [bitcoin_cohort_matrix_drilldown.sql](#bitcoin_cohort_matrix_drilldownsql)
  - [bitcoin_cohort_distribution_v2.sql](#bitcoin_cohort_distribution_v2sql)
  - [bitcoin_utxo_heuristics_v2.sql](#bitcoin_utxo_heuristics_v2sql)
  - [bitcoin_privacy_heuristics_v3.sql](#bitcoin_privacy_heuristics_v3sql)
- [Legacy Queries (Deprecated)](#legacy-queries-deprecated)
  - [bitcoin_utxo_heuristics.sql](#bitcoin_utxo_heuristicssql)
  - [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql-legacy)
  - [bitcoin_human_factor_scoring.sql](#bitcoin_human_factor_scoringsql)
  - [bitcoin_cohort_distribution.sql](#bitcoin_cohort_distributionsql)

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

### Unified Architecture (V2) - Recommended

| Query | Type | Dune Query ID | Description |
|-------|------|---------------|-------------|
| [bitcoin_tx_features_daily.sql](#bitcoin_tx_features_dailysql) | Base | TBD | Unified base query - computes ALL transaction features |
| [bitcoin_human_factor_scoring_v2.sql](#bitcoin_human_factor_scoring_v2sql) | Nested | TBD | Aggregates by day + score band |
| [bitcoin_human_factor_cohort_matrix.sql](#bitcoin_human_factor_cohort_matrixsql) | Nested | TBD | Cross-tabulation: score band × cohort |
| [bitcoin_cohort_matrix_drilldown.sql](#bitcoin_cohort_matrix_drilldownsql) | Nested (2-level) | TBD | Cohort drilldown with zero-fill densification |
| [bitcoin_cohort_distribution_v2.sql](#bitcoin_cohort_distribution_v2sql) | Nested | TBD | Aggregates by day + cohort |
| [bitcoin_utxo_heuristics_v2.sql](#bitcoin_utxo_heuristics_v2sql) | Nested | TBD | Aggregates by day + intent |
| [bitcoin_privacy_heuristics_v3.sql](#bitcoin_privacy_heuristics_v3sql) | Nested | TBD | Privacy analysis on "other" intent |

### Legacy Queries (Deprecated)

| Query | Blockchain | Dune Query ID | Description | Status |
|-------|------------|---------------|-------------|--------|
| [bitcoin_utxo_heuristics.sql](#bitcoin_utxo_heuristicssql) | Bitcoin | `query_6614095` | Classifies transactions by intent | **DEPRECATED** - Use V2 |
| [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql-legacy) | Bitcoin | TBD | Privacy analysis | **DEPRECATED** - Use V3 |
| [bitcoin_human_factor_scoring.sql](#bitcoin_human_factor_scoringsql) | Bitcoin | TBD | Human factor scoring | **DEPRECATED** - Use V2 |
| [bitcoin_cohort_distribution.sql](#bitcoin_cohort_distributionsql) | Bitcoin | TBD | Cohort distribution | **DEPRECATED** - Use V2 |

---

## Query Dependencies

### Unified Architecture (V2) - Recommended

The V2 architecture uses a single base query that computes ALL transaction features once, with 5 lightweight nested queries for different aggregation views:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  bitcoin_tx_features_daily.sql (BASE QUERY)                                 │
│  - Fetches bitcoin.inputs + bitcoin.outputs ONCE per execution              │
│  - Computes ALL features: counts, values, BDD, intent, privacy flags        │
│  - ALSO computes: human_factor_score, score_band, cohort, cohort_order      │
│  - Uses incremental processing with previous.query.result()                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ (ALL views are exactly 1 level deep)
       ┌──────────────┬───────────────┼───────────────┬──────────────┐
       │              │               │               │              │
       ▼              ▼               ▼               ▼              ▼
┌────────────┐ ┌────────────┐ ┌─────────────┐ ┌────────────┐ ┌────────────┐
│ human_     │ │ human_     │ │ cohort_     │ │ utxo_      │ │ privacy_   │
│ factor_v2  │ │ factor_    │ │ dist_v2     │ │ heur_v2    │ │ heur_v3    │
│            │ │ cohort_mat │ │             │ │            │ │            │
│ GROUP BY   │ │ GROUP BY   │ │ GROUP BY    │ │ GROUP BY   │ │ GROUP BY   │
│ day,band   │ │ day,band,  │ │ day,cohort  │ │ day,intent │ │ day,issue  │
│            │ │ cohort     │ │             │ │            │ │            │
│ (1 level)  │ │ (1 level)  │ │ (1 level)   │ │ (1 level)  │ │ (1 level)  │
└────────────┘ └─────┬──────┘ └─────────────┘ └────────────┘ └────────────┘
                     │
                     ▼
               ┌────────────┐
               │ cohort_    │
               │ matrix_    │
               │ drilldown  │
               │            │
               │ Filter +   │
               │ zero-fill  │
               │ (2 level)  │
               └────────────┘
```

**Key Design Decisions:**
- **Max 1-level nesting** - Avoids parasitic query costs (Dune nested queries have NO caching)
- **All derived fields in base** - Score, band, cohort computed once, not in nested queries
- **DRY principle** - Eliminates ~200 lines of duplicated aggregation logic

### Legacy Architecture (Deprecated)

The original architecture used independent queries that each fetched data separately:

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

**Note:** The legacy privacy heuristics query replicates the UTXO classification logic internally to filter to "other" transactions.

**Note:** The legacy human factor scoring query is standalone and processes all transactions independently.

---

## Unified Query Architecture (V2)

### bitcoin_tx_features_daily.sql

**Path:** `queries/bitcoin/bitcoin_tx_features_daily.sql`
**Dune Query ID:** TBD
**Type:** Base Query

**Description:**
Unified base query that computes ALL transaction-level features for downstream nested queries. Fetches data from `bitcoin.inputs` and `bitcoin.outputs` ONCE, then computes core metrics, human factor scoring, cohort classification, UTXO intent, and privacy flags.

**Author:** stefanopepe
**Created:** 2026-02-02

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Transaction inputs, BDD | `block_time`, `tx_id`, `value`, `block_height`, `spent_block_height`, `is_coinbase`, `address`, `type` |
| `bitcoin.outputs` | Transaction outputs | `block_time`, `tx_id`, `value`, `address`, `type` |

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transaction |
| `tx_id` | VARBINARY | Transaction identifier |
| `input_count` | BIGINT | Number of inputs |
| `output_count` | BIGINT | Number of outputs |
| `total_input_btc` | DOUBLE | Total input value (BTC) |
| `total_output_btc` | DOUBLE | Total output value (BTC) |
| `fee_btc` | DOUBLE | Transaction fee (BTC) |
| `dust_output_count` | BIGINT | Outputs < 546 sats |
| `round_value_count` | BIGINT | Outputs divisible by 0.001 BTC |
| `avg_days_held` | DOUBLE | Average BDD (Bitcoin Days Destroyed) |
| `human_factor_score` | BIGINT | Score 0-100 (automated to human) |
| `score_band` | VARCHAR | Score range (e.g., '50-60') |
| `score_band_order` | BIGINT | Numeric ordering 1-10 |
| `cohort` | VARCHAR | Volume cohort (Shrimps, Crab, etc.) |
| `cohort_order` | BIGINT | Numeric ordering 1-8 |
| `intent` | VARCHAR | UTXO classification |
| `has_address_reuse` | BOOLEAN | Output addr matches input addr |
| `output_type_mismatch` | BOOLEAN | Different output script types |

---

### bitcoin_human_factor_scoring_v2.sql

**Path:** `queries/bitcoin/bitcoin_human_factor_scoring_v2.sql`
**Type:** Nested Query (1 level deep)

**Description:**
Aggregates human factor scores by day and score band. Simple GROUP BY on the base query. Backward compatible with original query output schema.

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `score_band` | VARCHAR | Score range (e.g., '50-60') |
| `score_band_order` | BIGINT | Numeric ordering (1-10) |
| `tx_count` | BIGINT | Number of transactions |
| `btc_volume` | DOUBLE | Total BTC moved |
| `avg_score` | DOUBLE | Average score in band |

---

### bitcoin_human_factor_cohort_matrix.sql

**Path:** `queries/bitcoin/bitcoin_human_factor_cohort_matrix.sql`
**Type:** Nested Query (1 level deep)

**Description:**
Cross-tabulates human factor score bands with BTC volume cohorts. Enables analysis of how scoring varies by holder size. Matrix: 10 score bands × 8 cohorts = up to 80 cells per day.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `{{start_date}}` | DATE | Analysis start date |
| `{{end_date}}` | DATE | Analysis end date |

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `score_band` | VARCHAR | Score range (e.g., '50-60') |
| `score_band_order` | BIGINT | Numeric ordering (1-10) |
| `cohort` | VARCHAR | Holder cohort name |
| `cohort_order` | BIGINT | Numeric ordering (1-8) |
| `tx_count` | BIGINT | Number of transactions |
| `btc_volume` | DOUBLE | Total BTC moved |
| `avg_score` | DOUBLE | Average score in segment |

#### Cohort Definitions

| Cohort | BTC Range | Order |
|--------|-----------|-------|
| Shrimps | < 1 BTC | 1 |
| Crab | 1-10 BTC | 2 |
| Octopus | 10-50 BTC | 3 |
| Fish | 50-100 BTC | 4 |
| Dolphin | 100-500 BTC | 5 |
| Shark | 500-1,000 BTC | 6 |
| Whale | 1,000-5,000 BTC | 7 |
| Humpback | > 5,000 BTC | 8 |

---

### bitcoin_cohort_matrix_drilldown.sql

**Path:** `queries/bitcoin/bitcoin_cohort_matrix_drilldown.sql`
**Type:** Nested Query (2 levels deep)

**Description:**
Filters the cohort matrix to a single cohort via `{{cohort_filter}}` parameter and applies zero-fill densification across the day × score_band grid. Eliminates sparse-matrix gaps for dashboard line/area charts. 2-level nesting: drilldown → cohort_matrix (query_6663464) → base (query_6638509).

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `{{cohort_filter}}` | VARCHAR | Cohort to drill into (e.g., 'Shrimps (<1 BTC)') |
| `{{start_date}}` | DATE | Analysis start date |
| `{{end_date}}` | DATE | Analysis end date |

#### cohort_filter Valid Values

| Value | cohort_order |
|-------|-------------|
| `Shrimps (<1 BTC)` | 1 |
| `Crab (1-10 BTC)` | 2 |
| `Octopus (10-50 BTC)` | 3 |
| `Fish (50-100 BTC)` | 4 |
| `Dolphin (100-500 BTC)` | 5 |
| `Shark (500-1,000 BTC)` | 6 |
| `Whale (1,000-5,000 BTC)` | 7 |
| `Humpback (>5,000 BTC)` | 8 |

Configure as a Dune dropdown widget with these 8 values. The parameter value must match the exact cohort label string including the parenthetical BTC range.

#### Output Schema

| Column | Type | Zero-fill | Description |
|--------|------|-----------|-------------|
| `day` | DATE | spine | Date (every day in range) |
| `score_band` | VARCHAR | spine | Score range (all 10 bands per day) |
| `score_band_order` | BIGINT | spine | Numeric ordering (1-10) |
| `cohort` | VARCHAR | constant | Filtered cohort name |
| `cohort_order` | BIGINT | constant | Filtered cohort order |
| `tx_count` | BIGINT | 0 | Number of transactions |
| `btc_volume` | DOUBLE | 0.0 | Total BTC moved |
| `avg_score` | DOUBLE | NULL | Average score in segment |
| `avg_fee_btc` | DOUBLE | 0.0 | Average transaction fee in BTC |
| `total_fee_btc` | DOUBLE | 0.0 | Total fees paid in BTC |
| `tx_with_address_reuse` | BIGINT | 0 | Count of txs with address reuse |
| `tx_with_output_mismatch` | BIGINT | 0 | Count of txs with output type mismatch |
| `pct_address_reuse` | DOUBLE | NULL | Percentage of txs with address reuse |

#### Dense Output Guarantee

Unlike the parent cohort matrix (which is sparse), this query guarantees exactly `N_days × 10` rows in the output, where `N_days = date_diff('day', start_date, end_date)`. Every (day, score_band) cell is present. Missing cells are zero-filled with appropriate defaults (0 for counts, 0.0 for sums, NULL for averages and percentages).

#### Notes

- First 2-level nested query in the repository
- 2-level nesting means no caching at either level; execution cost is the full chain
- Configure `{{cohort_filter}}` as a Dune dropdown widget bound to the 8 cohort labels
- See `docs/dashboard_brief_human_factor_cohort_matrix.md` Section 10 for visualization guidance

---

### bitcoin_cohort_distribution_v2.sql

**Path:** `queries/bitcoin/bitcoin_cohort_distribution_v2.sql`
**Type:** Nested Query (1 level deep)

**Description:**
Aggregates transactions by day and BTC volume cohort. Simple GROUP BY on the base query. Backward compatible with original query output schema.

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `cohort` | VARCHAR | Holder cohort name |
| `cohort_order` | BIGINT | Numeric ordering (1-8) |
| `btc_moved` | DOUBLE | Total BTC moved |
| `tx_count` | BIGINT | Number of transactions |
| `spent_utxo_count` | BIGINT | Total UTXOs consumed |

---

### bitcoin_utxo_heuristics_v2.sql

**Path:** `queries/bitcoin/bitcoin_utxo_heuristics_v2.sql`
**Type:** Nested Query (1 level deep)

**Description:**
Aggregates transactions by day and intent classification. Simple GROUP BY on the base query. Backward compatible with original query output schema.

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `intent` | VARCHAR | Classified transaction intent |
| `tx_count` | BIGINT | Number of transactions |
| `sats_in` | DOUBLE | Total input value (BTC) |
| `sats_out` | DOUBLE | Total output value (BTC) |
| `avg_inputs` | DOUBLE | Average input count per tx |
| `avg_outputs` | DOUBLE | Average output count per tx |
| `median_inputs` | DOUBLE | Median input count per tx |
| `median_outputs` | DOUBLE | Median output count per tx |

---

### bitcoin_privacy_heuristics_v3.sql

**Path:** `queries/bitcoin/bitcoin_privacy_heuristics_v3.sql`
**Type:** Nested Query (1 level deep)

**Description:**
Analyzes privacy issues in transactions classified as "other" by the UTXO intent classification. Simplified version using flags computed in base query.

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `privacy_heuristic` | VARCHAR | Privacy issue detected |
| `tx_count` | BIGINT | Number of transactions |
| `sats_total` | DOUBLE | Total BTC involved |

#### Privacy Heuristics

| Heuristic | Description |
|-----------|-------------|
| `address_reuse` | Output address matches an input address |
| `output_type_mismatch` | 2-output tx with different script types |
| `no_issue_detected` | No privacy issues detected |

**Note:** Advanced heuristics (change_precision, UIH1, UIH2) require detailed output analysis not available in base query. Use the legacy `bitcoin_privacy_heuristics_v2.sql` for full analysis.

---

## Legacy Queries (Deprecated)

> **⚠️ DEPRECATED:** The queries below are superseded by the V2 unified architecture. They remain for backward compatibility but should not be used for new development.

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

## Ethereum / Lending Query Architecture

### Architecture Diagram

```
Tier 1 (Base, materialized):
  lending_action_ledger_unified (ID: 6687961)
    Unified stablecoin action ledger across Aave V3, Morpho Blue,
    Compound V3, Compound V2. Incremental with 1-day lookback.
         |
         v
Tier 2 (Base, materialized):
  lending_flow_stitching (ID: 6690272)
    Cross-protocol flow detection (borrow->supply).
    Materialized to break inline chain for downstream queries.
    Incremental with 1-day lookback.
         |
         +-----> lending_sankey_flows (nested)
         +-----> lending_loop_collateral_profile (nested, + collateral_ledger)
         |
         v
Tier 3 (Nested):
  lending_loop_detection (ID: TBD)
    Multi-hop loop detection via window functions (islands-and-gaps).
    Uniform 1-hour temporal constraint, arbitrary hop depth.
         |
         v
  lending_loop_metrics_daily (nested)
    Daily aggregated loop metrics + top protocol pair.
```

### lending_action_ledger_unified.sql

**Path:** `queries/ethereum/lending/lending_action_ledger_unified.sql`
**Dune Query ID:** 6687961
**Type:** Base Query (materialized with incremental processing)

**Description:**
Unified action ledger combining Aave V3, Morpho Blue, Compound V3, and Compound V2 lending events into a single normalized schema. Scoped to stablecoins (USDC, USDT, DAI, FRAX). Uses `previous.query.result()` for incremental processing with 1-day lookback.

**Author:** stefanopepe
**Created:** 2026-02-05

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Event timestamp |
| `block_date` | DATE | Event date |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Event log index |
| `protocol` | VARCHAR | Protocol identifier (aave_v3, morpho_blue, compound_v3, compound_v2) |
| `action_type` | VARCHAR | Action: supply/borrow/repay/withdraw/liquidation |
| `user_address` | VARBINARY | Entity performing action |
| `on_behalf_of` | VARBINARY | Beneficiary address |
| `entity_address` | VARBINARY | Canonical entity (COALESCE of on_behalf_of, user) |
| `asset_address` | VARBINARY | Underlying asset contract |
| `asset_symbol` | VARCHAR | Token symbol |
| `amount_raw` | UINT256 | Raw amount in asset decimals |
| `amount` | DOUBLE | Decimal-adjusted amount |
| `amount_usd` | DOUBLE | USD value at event time |

---

### lending_flow_stitching.sql

**Path:** `queries/ethereum/lending/lending_flow_stitching.sql`
**Dune Query ID:** 6690272
**Type:** Base Query (materialized with incremental processing)

**Description:**
Detects cross-protocol capital flows by stitching borrow events on Protocol P1 with supply events on Protocol P2. Supports same-transaction (atomic) flows and cross-transaction flows within a 2-minute window. Materialized to break the inline query chain for downstream consumers.

**Author:** stefanopepe
**Created:** 2026-02-05

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `flow_id` | VARCHAR | Unique flow identifier |
| `block_date` | DATE | Flow date |
| `entity_address` | VARBINARY | Entity executing the flow |
| `source_protocol` | VARCHAR | Protocol where borrow occurred |
| `dest_protocol` | VARCHAR | Protocol where supply occurred |
| `asset_address` | VARBINARY | Asset being moved |
| `asset_symbol` | VARCHAR | Token symbol |
| `borrow_tx_hash` | VARBINARY | Borrow transaction hash |
| `supply_tx_hash` | VARBINARY | Supply transaction hash |
| `borrow_time` | TIMESTAMP | Borrow timestamp |
| `supply_time` | TIMESTAMP | Supply timestamp |
| `time_delta_seconds` | INTEGER | Time between borrow and supply |
| `is_same_tx` | BOOLEAN | Whether flow occurred in same transaction |
| `amount` | DOUBLE | Flow amount (from borrow) |
| `amount_usd` | DOUBLE | USD value of flow |
| `flow_speed_category` | VARCHAR | atomic/near_instant/fast/delayed |

---

### lending_loop_detection.sql

**Path:** `queries/ethereum/lending/lending_loop_detection.sql`
**Dune Query ID:** TBD
**Type:** Nested Query

**Description:**
Detects multi-hop lending loops using window functions (islands-and-gaps pattern). A loop is a chain of flows where the destination protocol of one flow becomes the source of the next, within a 1-hour temporal window. Supports arbitrary hop depth.

**Author:** stefanopepe
**Created:** 2026-02-05

#### Detection Algorithm

1. **LAG()** previous flow's `dest_protocol` and `borrow_time` per entity
2. **Tag** each flow as continuation (source = prev dest AND within 1 hour) or chain start
3. **Running SUM** of chain starts assigns chain IDs (islands-and-gaps)
4. **GROUP BY** entity + chain_id to compute per-loop metrics

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `loop_id` | VARCHAR | Unique loop identifier (root flow_id) |
| `entity_address` | VARBINARY | Entity executing the loop |
| `start_date` | DATE | Loop initiation date |
| `end_date` | DATE | Always NULL (repay tracking not implemented) |
| `protocols_involved` | ARRAY(VARCHAR) | Array of protocols in loop path |
| `hop_count` | BIGINT | Number of protocol hops |
| `recursion_depth` | BIGINT | Same as hop_count |
| `root_tx_hash` | VARBINARY | First transaction in loop |
| `gross_borrowed_usd` | DOUBLE | Total USD borrowed across all hops |
| `loop_status` | VARCHAR | deep_loop (>=3) / standard_loop (2) / single_hop (1) |

---

### lending_loop_metrics_daily.sql

**Path:** `queries/ethereum/lending/lending_loop_metrics_daily.sql`
**Dune Query ID:** TBD
**Type:** Nested Query

**Description:**
Daily aggregated metrics for cross-protocol lending loops. Combines loop detection results with flow stitching data to produce loop counts, credit creation volumes, and top protocol pair analysis.

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date |
| `loops_started` | BIGINT | New loops initiated |
| `unique_loopers` | BIGINT | Distinct entities with loops |
| `gross_credit_created_usd` | DOUBLE | Total borrowed across all loops |
| `avg_recursion_depth` | DOUBLE | Average hop depth |
| `max_recursion_depth` | BIGINT | Deepest loop observed |
| `single_hop_loops` | BIGINT | Count of 1-hop loops |
| `double_hop_loops` | BIGINT | Count of 2-hop loops |
| `deep_loops` | BIGINT | Count of 3+ hop loops |
| `top_protocol_pair` | VARCHAR | Most common source->dest pair |
| `top_pair_volume_usd` | DOUBLE | Volume for top pair |

---

### lending_sankey_flows.sql

**Path:** `queries/ethereum/lending/lending_sankey_flows.sql`
**Dune Query ID:** TBD
**Type:** Nested Query

**Description:**
Edge list dataset for Sankey diagram visualization. Aggregates cross-protocol flows into daily edges with source/target nodes formatted as `{protocol}:{action}:{asset}`.

---

### lending_loop_collateral_profile.sql

**Path:** `queries/ethereum/lending/lending_loop_collateral_profile.sql`
**Dune Query ID:** TBD
**Type:** Nested Query

**Description:**
Joins cross-protocol flows with collateral positions to segment loop activity by backing asset category (BTC vs ETH vs LST vs mixed).

---

### lending_entity_loop_storyboard.sql

**Path:** `queries/ethereum/lending/lending_entity_loop_storyboard.sql`
**Dune Query ID:** TBD
**Type:** Nested Query

**Description:**
Time-ordered per-entity loop traces with running totals. Reads directly from the unified action ledger.

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
