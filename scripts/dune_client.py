"""
Dune Analytics API client wrapper.

Provides a simplified interface for executing queries and retrieving results
using direct HTTP calls (no external SDK dependency).
"""

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

API_BASE = "https://api.dune.com/api/v1"

# Dune rate-limit model (docs): low-limit endpoints are write-heavy (execute),
# high-limit endpoints are read-heavy (status/results).
PLAN_LIMITS = {
    "free": {"low_rpm": 15, "high_rpm": 40},
    "plus": {"low_rpm": 70, "high_rpm": 200},
    "enterprise": {"low_rpm": 350, "high_rpm": 1000},
}

_LAST_REQUEST_TS = {"low": 0.0, "high": 0.0}


@dataclass
class ExecutionResult:
    """Result of a query execution."""

    success: bool
    execution_id: str | None
    state: str
    rows: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    error: str | None = None
    execution_time_ms: int | None = None

    @property
    def is_empty(self) -> bool:
        """Check if result has no rows."""
        return self.row_count == 0


def _get_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        api_key = os.getenv("DUNE_API_KEY_FREE")
    if not api_key:
        raise ValueError(
            "DUNE_API_KEY (or optional DUNE_API_KEY_FREE) environment variable is not set. "
            "Copy .env.example to .env and add your API key."
        )
    return api_key


def _get_plan_limits() -> dict[str, int]:
    plan = os.getenv("DUNE_RATE_PLAN", "free").strip().lower()
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])


def _endpoint_bucket(method: str, path: str) -> str:
    # Execute endpoints are low-limit; status/results/read are high-limit.
    # We keep this narrow to endpoints used by this repository.
    p = path.split("?")[0]
    if method == "POST" and p in {"/sql/execute", "/query/execute"}:
        return "low"
    return "high"


def _throttle_for_bucket(bucket: str) -> None:
    limits = _get_plan_limits()
    rpm = limits["low_rpm"] if bucket == "low" else limits["high_rpm"]
    if rpm <= 0:
        return
    min_interval = 60.0 / float(rpm)
    now = time.time()
    elapsed = now - _LAST_REQUEST_TS[bucket]
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)


def _request(
    method: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    headers = {
        "X-Dune-API-Key": api_key,
        "Content-Type": "application/json",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    bucket = _endpoint_bucket(method, path)
    _throttle_for_bucket(bucket)
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            _LAST_REQUEST_TS[bucket] = time.time()
            if not body:
                return {}
            return json.loads(body)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="ignore")
        _LAST_REQUEST_TS[bucket] = time.time()
        raise RuntimeError(f"HTTP {e.code}: {raw}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error: {e}") from e


def _request_with_retry(
    method: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
    max_retries: int = 6,
) -> dict[str, Any]:
    """
    Retry transient rate-limit responses using bounded exponential backoff.
    """
    delay = 4.0
    for attempt in range(max_retries):
        try:
            return _request(method, path, api_key, payload)
        except RuntimeError as e:
            msg = str(e)
            if "HTTP 429" not in msg or attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 1.8, 30.0)
    return {}


def execute_sql(
    sql: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> ExecutionResult:
    """Execute raw SQL query via Dune API."""
    try:
        api_key = _get_api_key()

        # Dune endpoint for executing ad-hoc SQL.
        payload: dict[str, Any] = {"sql": sql, "performance": "medium"}
        if params:
            payload["query_parameters"] = params

        start = time.time()
        exec_resp = _request_with_retry("POST", "/sql/execute", api_key, payload)
        execution_id = str(exec_resp.get("execution_id", ""))
        if not execution_id:
            return ExecutionResult(
                success=False,
                execution_id=None,
                state="FAILED",
                rows=[],
                columns=[],
                row_count=0,
                error=f"Missing execution_id in response: {exec_resp}",
            )

        terminal_states = {
            "QUERY_STATE_COMPLETED",
            "QUERY_STATE_FAILED",
            "QUERY_STATE_CANCELLED",
            "QUERY_STATE_EXPIRED",
        }

        state = "QUERY_STATE_PENDING"
        last_status: dict[str, Any] = {}
        while time.time() - start < timeout_seconds:
            status = _request_with_retry("GET", f"/execution/{execution_id}/status", api_key)
            last_status = status
            state = str(status.get("state") or status.get("query_state") or state)
            if state in terminal_states:
                break
            time.sleep(2)

        if state != "QUERY_STATE_COMPLETED":
            err_msg = (
                last_status.get("error")
                or last_status.get("error_message")
                or last_status.get("message")
                or f"Execution not completed. Final state: {state}"
            )
            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                state=state,
                rows=[],
                columns=[],
                row_count=0,
                error=str(err_msg),
                execution_time_ms=int((time.time() - start) * 1000),
            )

        res = _request_with_retry("GET", f"/execution/{execution_id}/results", api_key)
        result_obj = res.get("result", {}) if isinstance(res, dict) else {}
        rows = result_obj.get("rows", []) if isinstance(result_obj, dict) else []
        columns = list(rows[0].keys()) if rows else []

        return ExecutionResult(
            success=True,
            execution_id=execution_id,
            state=state,
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_time_ms=int((time.time() - start) * 1000),
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            execution_id=None,
            state="FAILED",
            rows=[],
            columns=[],
            row_count=0,
            error=str(e),
        )


def execute_query(
    query_id: int,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> ExecutionResult:
    """Execute a saved Dune query by ID."""
    try:
        api_key = _get_api_key()
        payload: dict[str, Any] = {"query_id": query_id}
        if params:
            payload["query_parameters"] = params

        start = time.time()
        exec_resp = _request_with_retry("POST", "/query/execute", api_key, payload)
        execution_id = str(exec_resp.get("execution_id", ""))
        if not execution_id:
            return ExecutionResult(False, None, "FAILED", [], [], 0, f"Missing execution_id: {exec_resp}")

        terminal_states = {
            "QUERY_STATE_COMPLETED",
            "QUERY_STATE_FAILED",
            "QUERY_STATE_CANCELLED",
            "QUERY_STATE_EXPIRED",
        }
        state = "QUERY_STATE_PENDING"
        while time.time() - start < timeout_seconds:
            status = _request_with_retry("GET", f"/execution/{execution_id}/status", api_key)
            state = str(status.get("state") or status.get("query_state") or state)
            if state in terminal_states:
                break
            time.sleep(2)

        if state != "QUERY_STATE_COMPLETED":
            return ExecutionResult(False, execution_id, state, [], [], 0, f"Execution not completed. Final state: {state}")

        res = _request_with_retry("GET", f"/execution/{execution_id}/results", api_key)
        result_obj = res.get("result", {}) if isinstance(res, dict) else {}
        rows = result_obj.get("rows", []) if isinstance(result_obj, dict) else []
        columns = list(rows[0].keys()) if rows else []

        return ExecutionResult(
            success=True,
            execution_id=execution_id,
            state=state,
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_time_ms=int((time.time() - start) * 1000),
        )
    except Exception as e:
        return ExecutionResult(False, None, "FAILED", [], [], 0, str(e))


def get_latest_result(
    query_id: int,
    max_age_hours: int = 8,
) -> ExecutionResult:
    """Get latest cached result for a saved query."""
    try:
        api_key = _get_api_key()
        res = _request_with_retry("GET", f"/query/{query_id}/results?max_age_hours={max_age_hours}", api_key)
        result_obj = res.get("result", {}) if isinstance(res, dict) else {}
        rows = result_obj.get("rows", []) if isinstance(result_obj, dict) else []
        columns = list(rows[0].keys()) if rows else []
        return ExecutionResult(True, None, "QUERY_STATE_COMPLETED", rows, columns, len(rows))
    except Exception as e:
        return ExecutionResult(False, None, "FAILED", [], [], 0, str(e))


def get_query_details(query_id: int) -> dict[str, Any]:
    """Fetch query metadata by query ID."""
    api_key = _get_api_key()
    return _request_with_retry("GET", f"/query/{query_id}", api_key)


def get_execution_status(execution_id: str) -> dict[str, Any]:
    """Fetch execution status by execution ID."""
    api_key = _get_api_key()
    return _request_with_retry("GET", f"/execution/{execution_id}/status", api_key)
