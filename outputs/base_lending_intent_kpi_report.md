# Base Lending Intent KPI Report

- Generated at (UTC): `2026-03-12T15:30:15Z`
- Scope: chain=`base`, tier=`serving`, active_only=`True`
- Window: rolling `90` days (full refresh)

## Reliability

- Active queries in scope: **4**
- Core queries: **0**
- Serving queries: **4**
- Latest completed state count: **0/4**

## Smoke Execution

- Executed tests: **4**
- Passed: **1**
- Failed: **3**

## Efficiency Snapshot

- Sum of latest reported credits in scope: **0.000000**

| Query | Tier | Latest state | Credits | Runtime ms | Smoke status |
|---|---|---|---:|---:|---|
| `base_lending_entity_loop_storyboard` | `serving` | `None` |  |  | `FAIL` |
| `base_lending_loop_collateral_profile` | `serving` | `None` |  |  | `PASS` |
| `base_lending_loop_metrics_daily` | `serving` | `None` |  |  | `FAIL` |
| `base_lending_sankey_flows` | `serving` | `None` |  |  | `FAIL` |

## Trend-Quality Gate (Manual Checklist)

- Loop share trend looks directionally stable on rolling weekly view.
- Borrow volume by intent bucket has no unexplained structural discontinuities.
- Unique borrower trend by intent bucket remains interpretable after refactor.

## Notes

- Report is generated from latest query metadata and smoke-run outcomes.
- Use this report as rollout evidence for Base-first gates before extending horizon.
