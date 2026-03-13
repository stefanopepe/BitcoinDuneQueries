# Base Lending Intent Refactor Runbook

## Scope Policy

- Active execution scope is Base-only.
- Ethereum queries are frozen (`active=false`) and excluded from defaults.
- All active Base queries are configured with `refresh_window_days=90`.
- Full refresh is used (no state carry-over).

## Default Execution

Run Base core tier:

```bash
python3 -m scripts.smoke_runner --all --chain base --tier core --active-only true --window-days 90 --use-mcp-metrics
```

Run Base serving tier on demand:

```bash
python3 -m scripts.smoke_runner --all --chain base --tier serving --active-only true --window-days 90 --use-mcp-metrics
```

Generate inventory/report without running smoke tests:

```bash
python3 -m scripts.smoke_runner --inventory-only --chain base --tier all
```

## 6-Hour Cadence

Use the helper script:

```bash
scripts/run_base_full_refresh.sh
```

Optional serving refresh in same cycle:

```bash
RUN_SERVING=1 scripts/run_base_full_refresh.sh
```

Example cron entry (every 6 hours):

```cron
0 */6 * * * cd /Users/stefanopepe/Library/Mobile\ Documents/com~apple~CloudDocs/alpenproduct/AlpDuneQueriesCodex && /bin/zsh -lc 'source .env >/dev/null 2>&1; scripts/run_base_full_refresh.sh >> logs/base_refresh.log 2>&1'
```

## Artifacts

- `outputs/base_mcp_execution_inventory.json`
  - query-level runtime telemetry snapshot for current scope
  - entries may include `error` (`metadata_unavailable` or network/auth message) when live metadata cannot be retrieved
- `outputs/base_lending_intent_kpi_report.md`
  - rollout gate report (reliability, efficiency, trend-quality checklist)

## Rollback

- Execute full legacy scope when needed:

```bash
python3 -m scripts.smoke_runner --all --chain all --tier all --active-only false
```

- Keep Ethereum frozen while Base-first reliability and trend gates are unresolved.
