from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
import os

from src.dune_client import execute_sql
from src.extract.sql_loader import load_sql


def pct(x: float) -> str:
    return f"{x*100:.2f}%"


def main() -> None:
    lookback = int(os.getenv("LOOKBACK_DAYS", "30"))
    start = (datetime.now(UTC) - timedelta(days=lookback)).strftime("%Y-%m-%d %H:%M:%S")
    sql = load_sql("signal_coverage_sanity.sql", start_ts=start)
    rows = execute_sql(sql, performance="medium", timeout_seconds=1200)

    out = Path("outputs")
    out.mkdir(exist_ok=True)
    csv_path = out / "coverage_sanity.csv"
    md_path = out / "coverage_sanity.md"

    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    lines = []
    lines.append("# Coverage Sanity Check")
    lines.append("")
    lines.append(f"- As of: {datetime.now(UTC).isoformat()}")
    lines.append(f"- Lookback days: {lookback}")
    lines.append("- Baseline universe: all Morpho stable borrows (USDC/USDT/DAI) on Ethereum+Base")
    lines.append("- Cohort universe: BTC-backed Morpho borrowers (WBTC/cbBTC/tBTC collateral pre-borrow)")
    lines.append("- USD is approximated from stable token amount (peg assumption)")
    lines.append("")

    if not rows:
        lines.append("No rows returned.")
    else:
        headers = [
            "chain",
            "all_borrow_events",
            "cohort_borrow_events",
            "event_coverage_share",
            "all_borrowers",
            "cohort_borrowers",
            "borrower_coverage_share",
            "all_borrow_usd_approx",
            "cohort_borrow_usd_approx",
            "volume_coverage_share",
        ]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "---|" * len(headers))
        for r in rows:
            lines.append(
                "| " + " | ".join([
                    str(r.get("chain")),
                    str(r.get("all_borrow_events")),
                    str(r.get("cohort_borrow_events")),
                    pct(float(r.get("event_coverage_share") or 0.0)),
                    str(r.get("all_borrowers")),
                    str(r.get("cohort_borrowers")),
                    pct(float(r.get("borrower_coverage_share") or 0.0)),
                    f"{float(r.get('all_borrow_usd_approx') or 0.0):,.2f}",
                    f"{float(r.get('cohort_borrow_usd_approx') or 0.0):,.2f}",
                    pct(float(r.get("volume_coverage_share") or 0.0)),
                ]) + " |"
            )

        lines.append("")
        lines.append("Interpretation:")
        lines.append("- If `volume_coverage_share` is low, that reflects strict cohort scoping (BTC-backed), not missing Morpho borrow extraction.")
        lines.append("- If it is unexpectedly high/low versus prior runs, re-check token mappings and market decoding.")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"wrote {csv_path}")
    print(f"wrote {md_path}")


if __name__ == "__main__":
    main()
