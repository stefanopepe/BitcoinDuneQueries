# Coverage Sanity Check

- As of: 2026-02-26T11:32:29.228421+00:00
- Lookback days: 30
- Baseline universe: all Morpho stable borrows (USDC/USDT/DAI) on Ethereum+Base
- Cohort universe: BTC-backed Morpho borrowers (WBTC/cbBTC/tBTC collateral pre-borrow)
- USD is approximated from stable token amount (peg assumption)

| chain | all_borrow_events | cohort_borrow_events | event_coverage_share | all_borrowers | cohort_borrowers | borrower_coverage_share | all_borrow_usd_approx | cohort_borrow_usd_approx | volume_coverage_share |
|---|---|---|---|---|---|---|---|---|---|
| base | 41477 | 33529 | 80.84% | 12159 | 8927 | 73.42% | 240,338,347.53 | 211,849,140.97 | 88.15% |
| ethereum | 8733 | 1569 | 17.97% | 1336 | 358 | 26.80% | 1,331,909,357.89 | 370,869,707.86 | 27.84% |

Interpretation:
- If `volume_coverage_share` is low, that reflects strict cohort scoping (BTC-backed), not missing Morpho borrow extraction.
- If it is unexpectedly high/low versus prior runs, re-check token mappings and market decoding.