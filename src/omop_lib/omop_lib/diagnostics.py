"""User-scoped health probes against UC + the warehouse."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

from databricks.sdk import WorkspaceClient

from .config import Settings, settings
from .queries import OMOP_FUNCTIONS
from .warehouse import query, server_hostname


@dataclass
class Check:
    name: str
    status: str  # "ok" | "warn" | "fail"
    detail: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def _probe(name: str, fn: Callable[[], str]) -> Check:
    try:
        return Check(name=name, status="ok", detail=fn() or "")
    except Exception as exc:  # caught for diagnostics reporting
        return Check(
            name=name,
            status="fail",
            detail=f"{type(exc).__name__}: {exc}"[:400],
        )


def run(token: str, s: Settings | None = None) -> list[Check]:
    """Run the six diagnostic probes as `token`'s principal."""
    s = s or settings()

    def warehouse_state() -> str:
        w = WorkspaceClient(host=f"https://{server_hostname(s)}", token=token)
        wh = w.warehouses.get(id=s.warehouse_id)
        return f"state={wh.state}, size={wh.cluster_size or 'serverless'}"

    def select_one() -> str:
        t0 = time.perf_counter()
        rows = query("SELECT 1 AS ok", {}, token, s)
        dt = int((time.perf_counter() - t0) * 1000)
        if not rows or rows[0].get("ok") != 1:
            raise RuntimeError(f"unexpected response: {rows!r}")
        return f"{dt}ms round-trip"

    def catalog_use() -> str:
        rows = query(f"SHOW SCHEMAS IN `{s.catalog}`", {}, token, s)
        return f"{len(rows)} schema(s) visible"

    def schema_use() -> str:
        rows = query(
            f"SHOW TABLES IN `{s.catalog}`.`{s.omop_schema}`", {}, token, s
        )
        return f"{len(rows)} table(s) visible"

    def functions_visible() -> str:
        rows = query(
            f"SHOW USER FUNCTIONS IN `{s.catalog}`.`{s.omop_schema}` LIKE 'omop_*'",
            {},
            token,
            s,
        )
        found = {
            str(r.get("function") or r.get("Function") or "").split(".")[-1]
            for r in rows
        }
        missing = [f for f in OMOP_FUNCTIONS if f not in found]
        if missing:
            return (
                f"{len(OMOP_FUNCTIONS) - len(missing)}/{len(OMOP_FUNCTIONS)} "
                f"visible — missing: {', '.join(missing)}"
            )
        return f"all {len(OMOP_FUNCTIONS)} functions visible"

    def function_execute() -> str:
        rows = query(
            f"SELECT * FROM {s.fqn}.omop_top_conditions(:n)", {"n": 1}, token, s
        )
        return f"returned {len(rows)} row(s)"

    return [
        _probe("Warehouse reachable", warehouse_state),
        _probe("SELECT 1 round-trip", select_one),
        _probe(f"USE CATALOG `{s.catalog}`", catalog_use),
        _probe(f"USE SCHEMA `{s.omop_schema}`", schema_use),
        _probe("OMOP functions visible", functions_visible),
        _probe("EXECUTE omop_top_conditions(1)", function_execute),
    ]
