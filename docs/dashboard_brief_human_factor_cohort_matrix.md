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

The query provides deterministic integer sort keys. **Always use these -- never sort by the label strings.**

- `score_band_order` (1-10, low-to-high score)
- `cohort_order` (1-8, small-to-large volume)

**Dune chart settings:** In each visualization's configuration, set the "Sort" or "Order" field to the corresponding `*_order` column. For stacked bar series, set series ordering to `cohort_order` ascending (1=Shrimps at bottom). For area/line charts broken out by cohort, set series ordering the same way. If Dune's chart editor offers a "Category order" dropdown, select the `*_order` column rather than alphabetical. Lexicographic sorting produces wrong results (e.g., "Crab" before "Dolphin" alphabetically, but `cohort_order` 2 vs 5 numerically).

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

> **Boundary audit (2026-02-06).** The base query assigns cohorts via a cascading `CASE` with strict `<` comparisons. Effective intervals: `[0,1)`, `[1,10)`, `[10,50)`, `[50,100)`, `[100,500)`, `[500,1000)`, `[1000,5000)`, `[5000,inf)`. No double-counting, no gaps. Score bands use the same cascading `<` pattern. No action needed from the dashboard builder.

---

## 5. Dashboard visualizations

### 5.0 Zero-filling sparse cells

The matrix is sparse: many (day, cohort) and (day, score_band, cohort) cells have no transactions and therefore **no row in the result set** (not a row with zero). For stacked charts this causes visual gaps and unstable legend ordering rather than correctly showing zero-height segments. Apply this densification pattern before charting.

**For Visualization 5.1 (day x cohort):**

```sql
WITH
days AS (
    SELECT day
    FROM UNNEST(SEQUENCE(
        DATE '{{start_date}}',
        DATE '{{end_date}}' - INTERVAL '1' DAY,
        INTERVAL '1' DAY
    )) AS t(day)
),
cohorts AS (
    SELECT cohort, cohort_order
    FROM (VALUES
        ('Shrimps (<1 BTC)', 1), ('Crab (1-10 BTC)', 2),
        ('Octopus (10-50 BTC)', 3), ('Fish (50-100 BTC)', 4),
        ('Dolphin (100-500 BTC)', 5), ('Shark (500-1,000 BTC)', 6),
        ('Whale (1,000-5,000 BTC)', 7), ('Humpback (>5,000 BTC)', 8)
    ) AS t(cohort, cohort_order)
),
spine AS (
    SELECT d.day, c.cohort, c.cohort_order
    FROM days d CROSS JOIN cohorts c
),
actual AS (
    SELECT day, cohort, cohort_order,
           SUM(tx_count) AS tx_count, SUM(btc_volume) AS btc_volume
    FROM query_results
    GROUP BY day, cohort, cohort_order
)
SELECT
    s.day, s.cohort, s.cohort_order,
    COALESCE(a.tx_count, 0) AS tx_count,
    COALESCE(a.btc_volume, 0) AS btc_volume
FROM spine s
LEFT JOIN actual a ON s.day = a.day AND s.cohort_order = a.cohort_order
ORDER BY s.day, s.cohort_order
```

For the full matrix (day x score_band x cohort), add a `score_bands` CTE with all 10 bands and extend the CROSS JOIN to three dimensions. This is needed only if you build a heatmap or per-band breakdown.

**Normalize to % (optional):**

To show cohort proportions instead of absolute counts, add a window function:

```sql
-- append to the final SELECT
, tx_count * 100.0 / NULLIF(SUM(tx_count) OVER (PARTITION BY day), 0) AS tx_pct
```

This is useful for a "100% stacked" bar variant, which highlights proportional shifts even when total volume fluctuates day-to-day.

---

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
- **Apply zero-fill from Section 5.0 before charting.** Without it, missing (day, cohort) cells will cause visual gaps in the stacked bars rather than correctly showing zero-height segments, and legend/series ordering may shift day-to-day.

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

**Warning -- do not use a naive `AVG(avg_score)`.** The `avg_score` in each cell is already a mean over that cell's transactions. To reconstitute a correct global daily average you must weight by the cell's population. A naive `AVG(avg_score)` gives equal weight to a cell with 1 transaction and a cell with 100,000 transactions, producing a meaningless number.

**Data preparation (single-series, tx-count-weighted avg):**

```sql
SELECT
    day,
    SUM(avg_score * tx_count) / NULLIF(SUM(tx_count), 0) AS tx_weighted_avg_score
FROM query_results
GROUP BY day
ORDER BY day
```

**Data preparation (single-series, BTC-volume-weighted avg):**

```sql
SELECT
    day,
    SUM(avg_score * btc_volume) / NULLIF(SUM(btc_volume), 0) AS vol_weighted_avg_score
FROM query_results
GROUP BY day
ORDER BY day
```

BTC-volume weighting gives proportionally more influence to large transactions. A divergence between the tx-weighted and volume-weighted lines signals that large and small transactions have different score profiles -- this divergence is itself an analytical insight worth surfacing as a dashboard toggle or dual-line overlay.

**Data preparation (multi-series by cohort, tx-count-weighted):**

```sql
SELECT
    day,
    cohort,
    cohort_order,
    SUM(avg_score * tx_count) / NULLIF(SUM(tx_count), 0) AS tx_weighted_avg_score
FROM query_results
GROUP BY day, cohort, cohort_order
ORDER BY day, cohort_order
```

**Data preparation (multi-series by cohort, BTC-volume-weighted):**

```sql
SELECT
    day,
    cohort,
    cohort_order,
    SUM(avg_score * btc_volume) / NULLIF(SUM(btc_volume), 0) AS vol_weighted_avg_score
FROM query_results
GROUP BY day, cohort, cohort_order
ORDER BY day, cohort_order
```

**Configuration notes:**
- Y-axis range should be fixed at [0, 100] to maintain consistent visual scale.
- Add a horizontal reference line at score = 50 (the neutral baseline) to anchor interpretation.
- **Do not use stacked area for scores.** Scores are averages on a [0,100] scale -- stacking them produces a meaningless y-axis that sums to 400+ and implies scores are additive (they are not). Use a **single filled area** for the aggregate line (tx-weighted), with **overlaid thin lines** for per-cohort breakdowns. This lets the reader see both the global trend and cohort-level divergences without visual confusion.
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

**Date range constraint:** The base query (`query_6638509`) falls back to `2026-01-01` on first run. Data before that date is not available unless the fallback is manually adjusted in the base query's `checkpoint` CTE. Configure the dashboard date picker with a minimum selectable date of **2026-01-01** to prevent users from selecting a range that returns empty results. Selecting earlier dates will not cause an error, but the empty result set could be misinterpreted as "zero activity" rather than "no data processed."

---

## 7. Known data characteristics and caveats

1. **Sparse matrix.** Large cohorts (Shark, Whale, Humpback) have very few transactions daily. Expect many (day, band, cohort) cells to be **absent from the result set**, not present with zero values. For correct charting, apply the zero-fill pattern in Section 5.0 so that absent cells render as zero rather than as gaps.

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
