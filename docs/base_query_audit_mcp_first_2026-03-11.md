# Base Query Audit (MCP-First) for Cost + Architecture Optimization

Date: 2026-03-11  
Branch: `codex/base-query-audit-mcp-first-2026-03-11`

## 0) MCP-First Execution Log + Explicit Fallbacks

This repo defines an MCP-first policy in `README.md` and `CLAUDE.md`.  
For this session, live MCP execution was not possible from this environment.

- MCP check (attempted): `codex mcp list`
  - Result: no MCP servers configured (`dune_prod` missing).
  - Fallback used: local static SQL audit over `queries/registry.base.json`, `queries/base/lending/*.sql`, and `sql/*.sql`.
- Live Dune execution check (attempted): `set -a; source .env; python3 -m scripts.smoke_runner --test base_lending_action_ledger_unified --timeout 240`
  - Result: execution failed due sandbox network restriction (`Network error: <urlopen error [Errno 8] nodename nor servname provided, or not known>`), so no live runtime/token telemetry could be collected.
  - Additional context: all `dune_query_id` values in `queries/registry.base.json` are currently `null`, so nested query placeholder substitution is also blocked for end-to-end dependency execution.
  - Fallback used: static cost heuristics (scan breadth, join/window/cardinality risk, incremental shape, projection discipline) plus local smoke-test SQL review.

## 1) Query-by-Query Audit Table

### A. In-Scope Registry Queries (`queries/registry.base.json`)

| Query (name/path) | Current performance/cost signals | Anti-patterns found | Recommended rewrite | Expected cost impact | Risk/regression notes |
|---|---|---|---|---|---|
| `base_lending_action_ledger_aave_v3`<br>`queries/base/lending/lending_action_ledger_aave_v3.sql` | 249 lines; 11 CTEs; 5 source event scans; 2 `LEFT JOIN`; 9 `SELECT *`; incremental via `previous.query.result` | Date filter uses `CAST(date_trunc(...evt_block_time...))` repeatedly instead of direct partition column; token metadata join on full event stream; repeated `SELECT *` propagation | Use `evt_block_date` filter directly; restrict to tracked assets early; replace `tokens.erc20` join with small token dimension CTE/table; remove internal `SELECT *` | High | Potential behavior change if currently relying on fallback 18-decimals for unknown assets |
| `base_lending_action_ledger_morpho`<br>`queries/base/lending/lending_action_ledger_morpho.sql` | 241 lines; 14 CTEs; 5 event scans + `createmarket`; 2 `LEFT JOIN`; 9 `SELECT *`; incremental | Repeated wide unions; runtime JSON extraction from market params each run; row-level minute price join on full event set | Materialize `morpho_market_dim_base` (market_id -> loan_token); prune projected columns in each stage; prefilter price domain to `{asset, minute}` from event set | High | Low semantic risk if dimensions are exact; medium risk if market mapping refresh logic is wrong |
| `base_lending_action_ledger_unified`<br>`queries/base/lending/lending_action_ledger_unified.sql` | 410 lines; 20 CTEs; 10 event scans; 3 `LEFT JOIN`; 14 `SELECT *`; 11 `UNION ALL`; incremental | Duplicates extraction logic already present in protocol-specific ledgers; wide in-query orchestration; row-level price join at large cardinality | Refactor to `core` union of already-materialized protocol staging tables; isolate price enrichment in shared stage; remove placeholder wrapper map CTE until needed | High | Medium: canonical dataset changes require strict diff checks against current outputs |
| `base_lending_collateral_ledger`<br>`queries/base/lending/lending_collateral_ledger.sql` | 298 lines; 16 CTEs; 4 event scans + market map; 2 `LEFT JOIN`; 10 `SELECT *`; incremental | `stablecoin_addresses` CTE appears unused; dynamic `tokens.erc20` scan for ETH LST metadata each run; wide `SELECT *` unions | Replace dynamic metadata with maintained collateral dimension table; drop dead CTE; tighten projections; keep current 1-day incremental boundary | Medium-High | Low-medium: mostly structural; ensure collateral symbol/category mapping parity |
| `base_lending_flow_stitching`<br>`queries/base/lending/lending_flow_stitching.sql` | 264 lines; 11 CTEs; 1 upstream query dependency; 2 joins; window dedupe; incremental | Cross-tx borrow/supply join can explode for active entities/assets in 2-minute window; no block-distance bound despite available block numbers | Add block delta bound (for example <=10 blocks) in join; pre-bucket by minute/entity/asset; use nearest-supply match strategy with stricter tie-breaking | High | Medium-high: matching logic changes can shift loop counts/volume |
| `base_lending_entity_balance_sheet`<br>`queries/base/lending/lending_entity_balance_sheet.sql` | 114 lines; 2 heavy windows; full-history nested scan (no incremental/state reuse) | Recomputes running balances across entire history every run; final global `ORDER BY` on large output | Introduce daily incremental snapshot mart (carry-forward state); parameterize time window; drop expensive global sort in materialized output | Medium | Medium: must preserve running-balance continuity at partition boundaries |
| `base_lending_entity_loop_storyboard`<br>`queries/base/lending/lending_entity_loop_storyboard.sql` | 158 lines; 3 windows; full-history nested scan; distinct protocol filter | Full-history visualization query with expensive per-entity sequencing; no default date/entity parameters | Make this a presentation mart with required parameters (`start_date`, optional `entity_address`); precompute multi-protocol-entity set incrementally | High | Low-medium: mostly scope reduction; risk if dashboards expect full-history by default |
| `base_lending_loop_detection`<br>`queries/base/lending/lending_loop_detection.sql` | 143 lines; 3 windows + arrays; nested full-history scan | Chain detection over entire flow history each run; memory-heavy sort/window on `entity_address, borrow_time` | Partition chain detection by day with carry-over state (last destination protocol/time per entity); materialize chain facts | Medium-High | High: chain-state carry logic must be validated carefully |
| `base_lending_loop_metrics_daily`<br>`queries/base/lending/lending_loop_metrics_daily.sql` | 115 lines; joins two nested query outputs; `COUNT(DISTINCT)` and daily top-N window | Re-aggregates full history each run; depends on non-incremental upstreams | Consume daily marts (`loop_facts_daily`, `flow_pairs_daily`) only; add required date filter parameter for dashboard workloads | Medium | Low: aggregate logic is straightforward |
| `base_lending_sankey_flows`<br>`queries/base/lending/lending_sankey_flows.sql` | 93 lines; daily aggregation with `COUNT(DISTINCT)`; nested full-history scan | Full historical recomputation for visualization; no parameterized lookback | Materialize daily edge table incrementally and query recent horizon for frontend use | Medium | Low |
| `base_lending_loop_collateral_profile`<br>`queries/base/lending/lending_loop_collateral_profile.sql` | 203 lines; 2 windows; joins flow + collateral marts; high-cardinality entity/day join | Recomputes running collateral from raw changes each run; comment says “most recent prior date” but join is same-day only | Build `entity_collateral_snapshot_daily` as reusable mart with carry-forward; use as-of join semantics for flow date attribution | High | Medium-high: attribution logic may change historical category assignment |

### B. Directly Related SQL in `sql/` (Base-Including Signal Queries)

| Query/path | Current performance/cost signals | Anti-patterns found | Recommended rewrite | Expected cost impact | Risk/regression notes |
|---|---|---|---|---|---|
| `sql/signal_borrow_events.sql` | 178 lines; 11 CTEs; 13 joins; both Ethereum+Base unions; multiple `EXISTS` windows | Repeated chain unions; repeated temporal `EXISTS` checks for 1h/24h/7d; `SELECT *` final | Split chain-specific pipelines; compute event timeline once then derive window flags via joined interval table; project only required columns | High | Medium: loop flag definitions must stay identical |
| `sql/signal_borrow_outflows.sql` | 86 lines; 8 CTEs; transfer join over 7-day range; window ranking | Broad transfer scan per borrow; cross-chain union for Base-focused workloads | Parameterize chain default to `base`; prefilter transfers by date/token before borrow join; persist top recipients as intermediate | High | Low-medium |
| `sql/signal_coverage_sanity.sql` | 136 lines; 11 CTEs; repeated cohort existence checks | Duplicated market extraction and cohort logic already present elsewhere | Reuse shared staged models (`stable_borrows`, `btc_collateral_supplies`) and keep this as thin aggregate query | Medium-High | Low |
| `sql/signal_loop_significance_fast.sql` | 110 lines; 8 CTEs; “fast” path still recomputes key cohort datasets | Rebuilds borrower cohort and supply checks inline each run | Point to staged cohort tables and keep only final significance aggregation | Medium | Low |
| `sql/signal_loop_significance_probe.sql` | 117 lines; 8 CTEs; similar to fast version + price join | Near-duplicate of fast path with enrichment differences | Consolidate with fast query under one parameterized model (`mode = fast/probe`) plus shared staging | Medium | Low-medium |

## 2) Prioritized Action Plan

### Quick Wins (same logic, lower cost)

1. Replace `CAST(date_trunc('day', evt_block_time) AS DATE)` filters with native partition/date columns (`evt_block_date`) in Aave and any remaining event scans.
2. Remove internal `SELECT *` propagation in base ledgers and flow stitching; project only required columns at each CTE boundary.
3. Remove dead/unused CTEs (`stablecoin_addresses` in collateral ledger).
4. Parameterize dashboard-oriented nested queries with explicit time windows (`start_date`, `end_date`) and default lookback.
5. Add stricter join bounds in flow stitching (`block delta` + time delta) to control join explosion.

### Structural Rewrites (CTE/layout/join/filter changes)

1. Split protocol extraction into reusable staging models:
   - `stg_base_aave_actions_daily`
   - `stg_base_morpho_actions_daily`
   - `stg_base_morpho_market_dim`
   - `stg_base_prices_minute_filtered`
2. Rebuild `base_lending_action_ledger_unified` as a thin union over staged protocol actions.
3. Create `stg_base_collateral_actions_daily` and move collateral metadata out of runtime token scans.
4. Rework `base_lending_flow_stitching` to nearest-match semantics with bounded candidate set.

### Architecture-Level Refactors

1. Introduce layered model contract (`staging -> core -> marts`) and keep heavy logic out of presentation queries.
2. Add shared dimensions (`dim_base_tokens`, `dim_base_morpho_markets`) maintained independently from analytical marts.
3. Move running-balance/stateful logic into incremental snapshot marts instead of recomputing from raw actions.

## 3) Architecture Proposal (Target State)

### Model Layers

- `staging`
  - Protocol-normalized event feeds by day and chain.
  - Price subset model limited to in-use assets and date range.
- `core`
  - Canonical action ledgers (`core_base_lending_actions`, `core_base_collateral_actions`).
  - Canonical stitched flow facts (`core_base_lending_flows`).
- `marts`
  - `mart_base_loop_detection`
  - `mart_base_loop_metrics_daily`
  - `mart_base_sankey_edges_daily`
  - `mart_base_entity_collateral_snapshot_daily`
  - `mart_base_loop_collateral_profile`

### Reusable Intermediate Models

1. `dim_base_tokens` (`address`, `symbol`, `decimals`, `category`, `is_stablecoin`, `is_collateral`).
2. `dim_base_morpho_markets` (`market_id`, `loan_token`, `collateral_token`).
3. `stg_base_prices_minute_filtered` (only `{asset, minute}` needed by current run window).
4. `mart_base_entity_state_daily` (carry-forward collateral/debt by entity/protocol/asset).

### Parameter Standards

Use consistent Dune parameters in all non-base queries:

- `{{start_date}}` / `{{end_date}}` (date range)
- `{{lookback_days}}` (default for dashboards)
- `{{max_flow_minutes}}` (default 2)
- `{{max_flow_blocks}}` (default 10)
- `{{entity_address}}` (optional drilldown)

### Time-Window + Partition Strategy

1. Source event scans filter on native date partitions (`evt_block_date`) first.
2. Price joins filter with bounded `minute` range aligned to source partitions.
3. Incremental models use a configurable safety lookback (1-2 days) for late events/reorg tolerance.
4. Marts default to bounded historical windows for interactive dashboards.

### Materialization/Caching Strategy

1. Materialize all stage/core models that are reused by >1 downstream query.
2. Keep nested presentation queries thin and strictly aggregated.
3. Cache daily snapshots for running-state models; avoid full-history window recomputation.
4. Keep one canonical heavy flow table; derive all downstream metrics from it.

## 4) Implementation Roadmap

### Phase 1: Low-Risk Cost Cuts (1-2 days)

- Apply quick wins in place (partition filters, projection cleanup, dead CTE removal).
- Add parameters to nested visualization queries.

Validation gates:
- Smoke tests for all `base_lending_*` queries.
- Row-count and key aggregate parity vs baseline within agreed tolerance.

Rollback:
- Keep previous SQL versions under branch tags; revert per-query if parity checks fail.

### Phase 2: Structural Refactor (3-5 days)

- Build staging dims/models and refactor unified + collateral + flow stitching around them.
- Keep output contracts unchanged for downstream marts.

Validation gates:
- Contract checks (schema/column order/types).
- Metric diff checks (daily volume, loop counts, unique entities) against baseline.
- Token/cost comparison from MCP executions after environment is configured.

Rollback:
- Feature-flag cutover by switching downstream marts back to legacy upstream query IDs.

### Phase 3: Architecture Completion (3-5 days)

- Introduce incremental entity-state snapshots and as-of collateral attribution.
- Repoint loop metrics/sankey/storyboard to marts.

Validation gates:
- End-to-end dashboard parity tests.
- Regression budget thresholds (for example: key KPI drift <= 1% unless approved).

Rollback:
- Keep dual-run period (legacy and refactored) and route consumers to legacy until sign-off.

## 5) Definition of Done for “Great” Outcome

1. All Base registry queries have measured before/after cost (via MCP execution metrics).
2. Core models are materialized and reused; no repeated heavy extraction logic in marts.
3. Visualization queries (`storyboard`, `sankey`, `loop_metrics`) run on pre-aggregated marts.
4. Governance in place: parameter standard, model contracts, and regression gates.
