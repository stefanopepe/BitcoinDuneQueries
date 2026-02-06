# Technical Brief: Human Factor Cohort Matrix Dashboard

**For:** Dashboard builder / data scientist
**Source query:** `queries/bitcoin/bitcoin_human_factor_cohort_matrix.sql`
**Dune base query ID:** `query_6638509` (bitcoin_tx_features_daily)
**Date:** 2026-02-06

---

## 1. What this query produces

The cohort matrix query cross-tabulates two independent classification axes applied to every non-coinbase Bitcoin transaction:

| Axis | Field | Values |
|------|-------|--------|
| **Human factor score band** | `score_band` | 10 bands: `0-10`, `10-20`, ... `90-100` |
| **BTC volume cohort** | `cohort` | 8 tiers: Shrimps, Crab, Octopus, Fish, Dolphin, Shark, Whale, Humpback |

Each row in the result represents one (day, score_band, cohort) cell with three measures:

| Column | Type | Meaning |
|--------|------|---------|
| `tx_count` | BIGINT | Number of transactions in this cell |
| `btc_volume` | DOUBLE | Sum of `total_input_btc` across those transactions |
| `avg_score` | DOUBLE | Mean of the raw `human_factor_score` (0-100) within the cell |

Maximum cardinality: **80 cells per day** (10 bands x 8 cohorts). In practice many cells will be zero or absent, especially for large cohorts (Whale/Humpback) in extreme score bands.

### Ordering columns

The query provides deterministic sort keys so the dashboard doesn't need to parse label strings:

- `score_band_order` (1-10, low-to-high score)
- `cohort_order` (1-8, small-to-large volume)

Use these for axis ordering in all visualizations.

---

## 2. Data lineage

```
bitcoin.inputs + bitcoin.outputs
        │
        ▼
query_6638509  (bitcoin_tx_features_daily)
   - transaction-level row, one per tx per day
   - computes: human_factor_score, score_band, cohort, total_input_btc, etc.
   - incremental (1-day lookback via previous.query.result())
        │
        ▼
bitcoin_human_factor_cohort_matrix  (this query)
   - GROUP BY day, score_band, score_band_order, cohort, cohort_order
   - aggregates: COUNT(*), SUM(total_input_btc), AVG(human_factor_score)
```

The matrix query is a lightweight aggregation -- all heavy computation (scoring, cohort assignment, BDD, dust/round-value detection) happens in the base query.

---

## 3. How the human factor score works

Each transaction starts at **BASE_SCORE = 50** and accumulates adjustments:

| Signal | Condition | Weight |
|--------|-----------|--------|
| High fan-in | input_count > 50 | -15 |
| High fan-out | output_count > 50 | -15 |
| Round output values | any output divisible by 0.001 BTC | -5 |
| Dust outputs | any output < 546 sats | -10 |
| Simple structure | 1-in/1-out or 1-in/2-out | +10 |
| Non-round values | no round outputs | +5 |
| Moderate holder | avg days held 1-365 (via BDD) | +10 |
| Long-term holder | avg days held > 365 | +15 |

Final score is clamped to [0, 100]. Interpretation guide:

- **0-30:** likely automated (exchange hot wallets, mining pools, bots)
- **30-50:** probably automated
- **50-60:** ambiguous
- **60-80:** likely human-controlled
- **80-100:** strong human indicators (long-term HODLers)

---

## 4. How the cohort tiers work

Cohorts classify transactions by `total_input_btc` (sum of all input values in the transaction):

| Cohort | BTC range | `cohort_order` |
|--------|-----------|----------------|
| Shrimps | < 1 | 1 |
| Crab | 1 - 10 | 2 |
| Octopus | 10 - 50 | 3 |
| Fish | 50 - 100 | 4 |
| Dolphin | 100 - 500 | 5 |
| Shark | 500 - 1,000 | 6 |
| Whale | 1,000 - 5,000 | 7 |
| Humpback | > 5,000 | 8 |

These are **transaction-level** tiers, not wallet-level. A single wallet can produce transactions in different cohorts on different days.

---

## 5. Dashboard visualizations

### 5.1 Visualization 1 -- Stacked bar chart: Cohort distribution over time

**Purpose:** Show the daily mix of transaction sizes, revealing shifts in who is transacting on-chain.

| Property | Value |
|----------|-------|
| X-axis | `day` (date) |
| Y-axis | `tx_count` (or `btc_volume` as a toggle) |
| Series / color | `cohort` (8 series, ordered by `cohort_order`) |
| Chart type | Stacked bar |

**Data preparation:**

```sql
-- Collapse score_band dimension: aggregate up to (day, cohort)
SELECT
    day,
    cohort,
    cohort_order,
    SUM(tx_count) AS tx_count,
    SUM(btc_volume) AS btc_volume
FROM query_results
GROUP BY day, cohort, cohort_order
ORDER BY day, cohort_order
```

**Configuration notes:**
- Order bars by `cohort_order` (1=Shrimps at bottom, 8=Humpback at top) so the visual reads small-to-large bottom-to-top.
- Use a sequential or categorical color palette with 8 distinct colors. Suggested: light blue (Shrimps) through dark navy (Humpback).
- Consider offering a Y-axis toggle between `tx_count` (transaction count) and `btc_volume` (BTC moved). Count shows activity frequency; volume shows economic weight. The story changes significantly -- Shrimps dominate count, Whales dominate volume.
- The `{{start_date}}` and `{{end_date}}` parameters control the date range. Default: 30 days.

**What to look for:**
- Day-over-day shifts in cohort proportions (e.g., spike in Humpback activity may correlate with large OTC deals or exchange cold-wallet movements).
- Shrimps are the overwhelming majority by count, but often a minority by BTC volume -- toggling Y-axis reveals this contrast.

---

### 5.2 Visualization 2 -- Area chart: Human factor score over time

**Purpose:** Track the daily human-vs-automated character of Bitcoin transactions.

| Property | Value |
|----------|-------|
| X-axis | `day` (date) |
| Y-axis | weighted average human factor score |
| Chart type | Area (single series, or multi-series by cohort) |

**Data preparation (single-series, overall weighted avg):**

```sql
-- Compute daily volume-weighted average score
SELECT
    day,
    SUM(avg_score * tx_count) / SUM(tx_count) AS weighted_avg_score
FROM query_results
GROUP BY day
ORDER BY day
```

The weighting by `tx_count` is necessary because `avg_score` in each cell is already a mean within that cell; to reconstitute a correct global daily average, weight each cell's avg by its transaction count.

**Data preparation (multi-series, one line per cohort):**

```sql
SELECT
    day,
    cohort,
    cohort_order,
    SUM(avg_score * tx_count) / SUM(tx_count) AS weighted_avg_score
FROM query_results
GROUP BY day, cohort, cohort_order
ORDER BY day, cohort_order
```

**Configuration notes:**
- Y-axis range should be fixed at [0, 100] to maintain consistent visual scale.
- Add a horizontal reference line at score = 50 (the neutral baseline) to anchor interpretation.
- If using multi-series (one line per cohort), stack as an area chart or overlay as lines. Area stacking doesn't make semantic sense here (scores don't sum), so **overlaid lines** or a **single filled area** for the aggregate is better.
- Color the area using a gradient or threshold coloring: red zone (0-30), amber (30-60), green (60-100) to make the human/automated boundary intuitive.

**What to look for:**
- Sustained drops in the weighted avg score may indicate growing bot/exchange activity.
- Per-cohort divergences: Shrimps typically score higher (more human-like), Humpbacks lower (more likely institutional/automated). If a cohort's score suddenly shifts, that's an analytical signal worth investigating.

---

## 6. Parameters

The query accepts two Dune parameters:

| Parameter | Type | Default behavior | Dashboard widget |
|-----------|------|------------------|------------------|
| `{{start_date}}` | DATE | 30 days before today | Date picker |
| `{{end_date}}` | DATE | Today | Date picker |

Both visualizations share the same date window. Wire a single date range selector to both parameters.

---

## 7. Known data characteristics and caveats

1. **Sparse matrix.** Large cohorts (Shark, Whale, Humpback) have very few transactions daily. Expect many (day, band, cohort) cells to be missing, not zero. Handle missing cells as zero in aggregations.

2. **Transaction-level, not entity-level.** A single exchange wallet can generate thousands of Humpback-tier transactions. These are not 5,000 distinct whales -- they may be one entity. This query does not de-duplicate by address or entity.

3. **Score is heuristic, not definitive.** The scoring model is based on published academic heuristics (Meiklejohn 2013, Schnoering 2024, etc.) but any individual transaction's score is an estimate. Aggregate trends are more reliable than individual data points.

4. **BDD approximation.** Bitcoin Days Destroyed uses `(block_height - spent_block_height) / 144` to estimate holding time in days. The 144-blocks-per-day constant is an average; actual block intervals vary.

5. **Coinbase exclusion.** Mining reward transactions are excluded from all analysis. They are neither human nor automated in the sense this model measures.

6. **Incremental base query.** The base query (`query_6638509`) uses `previous.query.result()` for incremental processing with a 1-day lookback. On first run or after a gap, it only processes data from its fallback date (`2026-01-01`) forward. Historical data before that date is not available unless the fallback is adjusted.

---

## 8. Extending the dashboard

The same base query (`query_6638509`) powers four other aggregation views that can be added as additional dashboard panels without extra data scanning cost:

| Query file | Aggregation | Potential visualization |
|------------|-------------|------------------------|
| `bitcoin_human_factor_scoring_v2.sql` | day + score_band | Score distribution histogram per day |
| `bitcoin_cohort_distribution_v2.sql` | day + cohort | Cohort volume treemap or pie chart |
| `bitcoin_utxo_heuristics_v2.sql` | day + intent | Intent breakdown (consolidation, fan-out, CoinJoin, etc.) |
| `bitcoin_privacy_heuristics_v3.sql` | day + privacy_heuristic | Privacy issue prevalence over time |

All are 1-level nested queries on the same base, so adding them to the dashboard has no marginal data-fetch cost on Dune.

---

## 9. Questions for the dashboard builder

When you return with expansion ideas, the following dimensions are straightforward to add from the existing base query without new data sources:

- **Filter by intent.** The base query includes an `intent` column (consolidation, fan_out_batch, coinjoin_like, self_transfer, change_like_2_outputs, other). A new nested query could produce the matrix filtered to specific intents.
- **Fee analysis.** `fee_btc` is available per transaction. A fee-weighted view of cohorts or score bands could be added.
- **Privacy overlay.** `has_address_reuse` and `output_type_mismatch` are boolean flags on every transaction. These could be used as additional filter dimensions or secondary metrics.
- **BDD distribution.** `avg_days_held` is available per transaction and could power a holding-time histogram or scatter plot.

Additions that would require new data sources or base query changes:
- USD-denominated values (requires `prices.usd` join)
- Entity/wallet-level aggregation (requires address clustering or label tables)
- Mempool/fee-rate analysis (not in `bitcoin.inputs`/`bitcoin.outputs`)
