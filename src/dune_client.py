from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

API_BASE = "https://api.dune.com/api/v1"


@dataclass
class DuneExecution:
    execution_id: str
    state: str


def _get_api_key(mode: str = "exec") -> str:
    load_dotenv()
    if mode not in {"exec", "write"}:
        raise ValueError(f"Unknown API key mode: {mode}")

    # Canonical key for all operations.
    api_key = os.getenv("DUNE_API_KEY")
    if api_key:
        return api_key

    # Optional free-tier key for low-priority read/exec workloads only.
    if mode == "exec":
        free_api_key = os.getenv("DUNE_API_KEY_FREE")
        if free_api_key:
            return free_api_key

    if mode == "write":
        raise ValueError("DUNE_API_KEY environment variable is not set")
    raise ValueError("DUNE_API_KEY or DUNE_API_KEY_FREE environment variable is not set")


def _request(method: str, path: str, api_key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        if e.code == 429:
            raise RuntimeError("DUNE_RATE_LIMIT")
        raise RuntimeError(f"Dune HTTP {e.code}: {body}") from e


def _request_with_backoff(
    method: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
    max_retries: int = 8,
) -> dict[str, Any]:
    delay = 2.0
    for attempt in range(max_retries):
        try:
            return _request(method, path, api_key, payload)
        except RuntimeError as e:
            if str(e) != "DUNE_RATE_LIMIT" or attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 1.8, 30.0)
    return {}


def execute_sql(sql: str, performance: str = "medium", timeout_seconds: int = 1200) -> list[dict[str, Any]]:
    api_key = _get_api_key("exec")
    payload = {"sql": sql, "performance": performance}
    exec_resp = _request_with_backoff("POST", "/sql/execute", api_key, payload)
    execution_id = str(exec_resp.get("execution_id", ""))
    if not execution_id:
        raise RuntimeError(f"Missing execution_id in Dune response: {exec_resp}")
    print(f"[dune] execution_id={execution_id}", flush=True)

    terminal_states = {
        "QUERY_STATE_COMPLETED",
        "QUERY_STATE_FAILED",
        "QUERY_STATE_CANCELLED",
        "QUERY_STATE_EXPIRED",
    }
    state = "QUERY_STATE_PENDING"
    start = time.time()
    while time.time() - start < timeout_seconds:
        status = _request_with_backoff("GET", f"/execution/{execution_id}/status", api_key)
        state = str(status.get("state") or status.get("query_state") or state)
        if state in terminal_states:
            break
        time.sleep(5)

    if state != "QUERY_STATE_COMPLETED":
        raise RuntimeError(f"Dune execution {execution_id} ended in state {state}")

    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 20000
    while True:
        qs = urllib.parse.urlencode({"limit": page_size, "offset": offset})
        page = _request_with_backoff("GET", f"/execution/{execution_id}/results?{qs}", api_key)
        result_obj = page.get("result", {}) if isinstance(page, dict) else {}
        page_rows = result_obj.get("rows", []) if isinstance(result_obj, dict) else []
        rows.extend(page_rows)
        if len(page_rows) < page_size:
            break
        offset += page_size
    return rows
