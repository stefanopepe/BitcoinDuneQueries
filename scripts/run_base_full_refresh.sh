#!/usr/bin/env bash
set -euo pipefail

WINDOW_DAYS="${WINDOW_DAYS:-90}"
TIMEOUT="${TIMEOUT:-600}"
RUN_SERVING="${RUN_SERVING:-0}"

python3 -m scripts.smoke_runner \
  --all \
  --chain base \
  --tier core \
  --active-only true \
  --window-days "$WINDOW_DAYS" \
  --timeout "$TIMEOUT" \
  --use-mcp-metrics

if [[ "$RUN_SERVING" == "1" ]]; then
  python3 -m scripts.smoke_runner \
    --all \
    --chain base \
    --tier serving \
    --active-only true \
    --window-days "$WINDOW_DAYS" \
    --timeout "$TIMEOUT" \
    --use-mcp-metrics
fi
