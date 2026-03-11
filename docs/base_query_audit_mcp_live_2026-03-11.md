# Base Query Audit (MCP Live) - 2026-03-11

Date: 2026-03-11  
Repo scope: `queries/registry.base.json`, `queries/base/lending/*.sql`, `sql/*.sql`

## 1) MCP-First Status and Scope

- `dune_prod` MCP server is available and was used as primary source of live context.
- Base registry currently has **no Dune IDs**: all `dune_query_id` in `queries/registry.base.json` are `null`.
- Base nested queries reference placeholders like `query_<BASE_LENDING_FLOW_STITCHING_ID>`; these cannot be executed through MCP `getDuneQuery` / `executeQueryById` until IDs are populated.

### Explicit fallback notes (required)

1. Base query execution by ID is unsupported right now (no IDs in registry).
- Fallback used: static SQL audit + execution telemetry from mapped published counterparts in `queries/registry.ethereum.json` (same logical query family).

2. MCP `listBlockchains` call failed (`404 facet field not found`).
- Fallback used: `searchTables` on concrete table families/schemas.

3. MCP `searchDocs` failed (`Tool SearchDuneDocs not found`).
- Fallback used: direct MCP table metadata + local SQL inspection.

4. MCP `getTableSize` on decoded tables (for example `aave_v3_base.pool_evt_borrow`, `morpho_blue_base.morphoblue_evt_borrow`) returned `No TableScan/ScanProject entries found`.
- Fallback used: `searchTables` schema/metadata plus size checks on large spell/canonical tables (`dex.trades`, `tokens.transfers`, `base.traces`, `base.creation_traces`, `prices.minute`).

## 2) Query-by-Query Audit Table

### A) Registry Base Queries (`queries/registry.base.json`)

| Query | Live execution check (MCP) | Performance/cost signals | Main issues | Priority actions |
|---|---|---|---|---|
| `base_lending_action_ledger_morpho` | Direct Base check unavailable (no ID). Mapped counterpart `6708253`: `QUERY_STATE_COMPLETED`, latest execution credits `0.009`. | 241 lines, 14 CTEs, 9 `SELECT *`, 13 joins. Uses `evt_block_date` filters correctly. | Recomputes market mapping via JSON extraction each run; row-level `prices.usd` join; wide unions. | Materialize `dim_base_morpho_markets`; prune projections; move to `prices.minute` subset stage. |
| `base_lending_action_ledger_aave_v3` | Direct Base check unavailable. Mapped counterpart `6707805`: completed, credits `28.93`. | 249 lines, 11 CTEs, 9 `SELECT *`, 8 joins. | Repeated `CAST(date_trunc('day', evt_block_time) AS DATE)` filters instead of direct `evt_block_date`; token join on full stream. | Replace with `evt_block_date` predicates; early asset filter; reduce `SELECT *`; prebuild token dimension. |
| `base_lending_action_ledger_unified` | Direct Base check unavailable. Mapped counterpart `6687961`: completed, credits `0.715`. | 410 lines, 20 CTEs, 14 `SELECT *`, 22 joins, 11 `UNION ALL`. | Very wide orchestration; empty `wrapper_to_underlying` table pattern on Base; heavy repeated enrichment logic. | Split to protocol staging models and thin union core; remove empty wrapper branch until needed; centralize price enrichment. |
| `base_lending_collateral_ledger` | Direct Base check unavailable. Mapped counterpart `6707791`: completed, credits `15.285`. | 298 lines, 16 CTEs, 10 `SELECT *`, 11 joins. | `stablecoin_addresses` CTE declared but unused; runtime metadata/price enrichment in hot path. | Remove dead CTE; stage collateral dimension; stage price subset and narrow selected columns. |
| `base_lending_flow_stitching` | Direct Base check unavailable. Mapped counterpart `6690272`: completed, credits `52.52`. | 264 lines, 11 CTEs, 5 `SELECT *`, cross-join matching logic, incremental wrapper. | Comment says 10-block/2-minute window but SQL enforces only time; high join explosion risk for active entities. | Add block-distance constraint and deterministic nearest-match tie-breaker; prebucket by minute/entity/asset. |
| `base_lending_entity_balance_sheet` | Direct Base check unavailable. Mapped counterpart `6708623`: completed, credits `14.685`. | 114 lines, 2 windowed running sums; full-history recomputation. | Full-history cumulative windows each run; expensive global ordering for materialized result. | Build incremental daily state snapshot (`entity_asset_state_daily`), then query delta + carry-forward. |
| `base_lending_loop_detection` | Direct Base check unavailable. Mapped counterpart `6702204`: completed, credits `56.332`. | 143 lines, 3 window passes, array aggregations. | Full-history chain segmentation every run; memory-heavy partition sort by entity/time. | Incremental chain-state mart with carry-over state per entity; bounded rebuild windows. |
| `base_lending_loop_metrics_daily` | Direct Base check unavailable. Mapped counterpart `6708794`: completed, credits `103.767`. | 115 lines, nested aggregates + top-N window. | Re-aggregates historical loops/flows; highest observed execution credits among mapped counterparts. | Feed from pre-aggregated daily marts only; parameterize date windows for dashboard usage. |
| `base_lending_sankey_flows` | Direct Base check unavailable. Mapped counterpart `6708650`: completed, credits `50.826`. | 93 lines; daily aggregation with distinct counts. | Full-history recompute for visualization edge list. | Materialize daily edge mart incrementally; default dashboard lookback window. |
| `base_lending_entity_loop_storyboard` | Direct Base check unavailable. Mapped counterpart `6708643`: completed, credits `27.73`. | 158 lines, 3 windows, full-history sequencing. | Heavy visualization query with no default date/entity scope. | Add required `start_date` and optional `entity_address`; move to precomputed storyboard mart. |
| `base_lending_loop_collateral_profile` | Direct Base check unavailable. Mapped counterpart `6708668`: completed, credits `66.073`. | 203 lines, 2 windows, flow+collateral join. | SQL comment says “most recent prior date” but join is same-day (`ec.block_date = f.block_date`) in Base version. | Implement as-of join (`<=`) with row_number dedupe; persist daily collateral snapshots. |

### B) Related `sql/*.sql`

| Query | MCP discovery + table cost context | Performance/cost signals | Main issues | Priority actions |
|---|---|---|---|---|
| `sql/signal_borrow_events.sql` | Uses `dex.trades` (partitioned by `block_month`, `blockchain`, `project`; table size ~`906.131 GB`) and `prices.usd`. | 178 lines, 11 CTEs, 13 joins, repeated temporal `EXISTS`. | Repeated cohort logic and multi-window `EXISTS` scans; broad DEX joins. | Stage borrower cohort once, then derive 1h/24h/7d flags from staged events; enforce block-date bounded predicates. |
| `sql/signal_borrow_outflows.sql` | Uses `tokens.transfers` (~`15,737.733 GB`) + `base/ethereum.creation_traces` (`188.454 GB` on Base). | 86 lines, 8 CTEs, transfer join over 7-day windows. | Very large transfer scan risk; predicates are timestamp-range joins without explicit partition gating. | Add explicit `block_date` predicates and chain prefilter CTEs; prefilter token universe before transfer join. |
| `sql/signal_coverage_sanity.sql` | Uses morpho decoded tables + `tokens.erc20` (partitioned by blockchain). | 136 lines, 11 CTEs, repeated cohort derivations. | Near-duplicate logic with other signal files. | Consolidate shared staging (`stable_borrows`, `btc_collateral_supplies`) and keep this as thin QA aggregate. |
| `sql/signal_loop_significance_fast.sql` | Uses morpho decoded + `tokens.erc20`. | 110 lines, 8 CTEs. | Rebuilds same borrower/supply cohorts inline each run. | Consume shared staged cohorts; leave only significance computation. |
| `sql/signal_loop_significance_probe.sql` | Same as fast plus `prices.usd`. | 117 lines, 8 CTEs, extra enrichment joins. | Duplication with fast variant and repeated enrichment path. | Unify fast/probe via parameterized mode and common staging. |

## 3) Prioritized Action Plan

1. **Unblock Base live execution (critical)**
- Populate `dune_query_id` for all `base_lending_*` entries in `queries/registry.base.json`.
- This is the gating item for direct MCP execution checks and reliable incremental chain execution.

2. **Fix highest-cost query family first**
- Refactor `base_lending_loop_metrics_daily`, `base_lending_loop_collateral_profile`, and `base_lending_flow_stitching` (mapped credits: `103.767`, `66.073`, `52.52`).

3. **Partition pruning fixes on hot scans**
- Replace Aave `CAST(date_trunc(...evt_block_time...))` filters with native `evt_block_date`.
- Add explicit `block_date` constraints in `sql/signal_borrow_outflows.sql` and DEX/transfer joins.

4. **Remove dead/empty logic immediately**
- Drop unused `stablecoin_addresses` in collateral ledger.
- Remove empty `wrapper_to_underlying` branch in Base unified until Base wrappers are actually added.

5. **Staging reuse to kill duplication**
- Introduce shared staged models for borrower cohorts and collateral cohorts used across `sql/signal_*`.

6. **As-of correctness fix**
- Correct `base_lending_loop_collateral_profile` to real as-of join semantics (`<=` + latest prior row).

## 4) Architecture Proposal (Target State)

### Layered model contract

1. `staging`
- `stg_base_morpho_markets`
- `stg_base_lending_actions_aave`
- `stg_base_lending_actions_morpho`
- `stg_base_collateral_actions`
- `stg_prices_minute_filtered` (bounded by active assets + date window)

2. `core`
- `core_base_lending_actions_unified`
- `core_base_lending_flows`
- `core_base_collateral_positions_daily`

3. `marts`
- `mart_base_loop_detection_daily`
- `mart_base_loop_metrics_daily`
- `mart_base_sankey_edges_daily`
- `mart_base_entity_storyboard_daily`
- `mart_base_loop_collateral_profile_daily`

### Execution and dependency contract

- All nested Base queries should reference numeric `query_<id>` via registry substitution only.
- Registry IDs become mandatory for promotion to "production-ready".
- Add CI gate: fail if any Base nested query contains unresolved `query_<BASE_*_ID>` or registry ID is null.

### Parameter standard

- `{{start_date}}`, `{{end_date}}`
- `{{lookback_days}}`
- `{{max_flow_minutes}}`
- `{{max_flow_blocks}}`
- `{{entity_address}}` (optional)

## 5) Implementation Roadmap

### Phase 0 (same day): ID and execution observability unblock

- Set `dune_query_id` for all Base registry queries.
- Verify with MCP `getDuneQuery` and pull latest execution metadata for each.
- Deliverable: fully executable Base dependency chain.

### Phase 1 (1-2 days): low-risk cost cuts

- Partition predicate corrections (`evt_block_date`/`block_date`).
- Remove dead CTEs and empty wrapper logic.
- Restrict `SELECT *` to explicit projection in base ledgers and stitching.
- Deliverable: lower scan cost without output-contract changes.

### Phase 2 (3-5 days): structural refactor

- Introduce staging/core models and rewire unified, collateral, and flow stitching.
- Enforce as-of collateral attribution in loop collateral profile.
- Deliverable: reduced duplicate computation and stable intermediate contracts.

### Phase 3 (3-5 days): mart-focused serving layer

- Shift storyboard/sankey/metrics to pre-aggregated daily marts with bounded lookback defaults.
- Add regression checks for KPI parity (volume, loop count, unique entities).
- Deliverable: dashboard-friendly query latency + reduced credit burn.

## 6) Concrete MCP Tool Calls Made and Key Findings

### Connectivity and capability checks

- `list_mcp_resources(server="dune_prod")`
- Finding: server reachable; resources available.

- `listBlockchains(...)`
- Finding: failed with 404 facet-field error.
- Fallback: relied on `searchTables` for concrete chain/table discovery.

- `searchDocs(...)`
- Finding: tool not found in this MCP deployment.
- Fallback: used direct table metadata + local SQL review.

### Table discovery / schema verification

- `searchTables(query="pool_evt_supply", schemas=["aave_v3_base"], includeSchema=true)`
- Finding: Base Aave decoded events available; `evt_block_date` present.

- `searchTables(query="morphoblue_evt_borrow", schemas=["morpho_blue_base"], includeSchema=true)`
- Finding: Base Morpho decoded events available; `evt_block_date` present.

- `searchTables(query="dex.trades", schemas=["dex"], includeSchema=true, includeMetadata=true)`
- Finding: partition columns include `block_month`, `blockchain`, `project`.

- `searchTables(query="tokens.erc20", schemas=["tokens"], includeSchema=true, includeMetadata=true)`
- Finding: partitioned by `blockchain`.

- `searchTables(query="creation_traces", categories=["canonical"], blockchains=["base","ethereum"], includeSchema=true)`
- Finding: `base.creation_traces` and `ethereum.creation_traces` present with `block_date` partition.

### Table size / scan-risk evidence

- `getTableSize("tokens.transfers")` -> ~`15,737.733 GB`.
- `getTableSize("dex.trades")` -> ~`906.131 GB`.
- `getTableSize("base.traces")` -> ~`10,245.79 GB`.
- `getTableSize("base.creation_traces")` -> ~`188.454 GB`.
- `getTableSize("prices.minute")` -> ~`414.245 GB`.
- `getTableSize("aave_v3_base.pool_evt_borrow")`, `getTableSize("morpho_blue_base.morphoblue_evt_borrow")`
- Finding: no scan nodes returned (capability limitation for decoded-table sizing).

### Query execution checks (where supported)

- `getDuneQuery` called for mapped counterpart IDs:
`6708253, 6707805, 6687961, 6708623, 6690272, 6702204, 6708794, 6708650, 6708643, 6707791, 6708668`.
- Finding: all report `latest_execution_state = QUERY_STATE_COMPLETED`.

- `getExecutionResults(limit=1, timeout=0)` for each latest execution ID.
- Finding: all completed; latest execution credits sampled:
  - `6708794` (loop metrics daily): `103.767`
  - `6708668` (loop collateral profile): `66.073`
  - `6702204` (loop detection): `56.332`
  - `6690272` (flow stitching): `52.52`
  - `6708650` (sankey): `50.826`
  - `6708643` (storyboard): `27.73`
  - `6707805` (Aave ledger): `28.93`
  - `6707791` (collateral ledger): `15.285`
  - `6708623` (entity balance): `14.685`
  - `6687961` (unified ledger): `0.715`
  - `6708253` (Morpho ledger): `0.009`

### Billing context

- `getUsage()`
- Finding: current billing period `2026-03-05` to `2026-04-05`; `creditsUsed = 0` in this MCP context.

## 7) Key Audit Conclusion

- The largest blocker is **missing Base query IDs** in registry, not SQL correctness alone.
- Cost pressure is concentrated in flow/loop nested layers and large transfer/trade joins.
- Immediate ROI comes from: ID registration, partition pruning, and staging-model reuse.
