"""SQL warehouse connection + query helpers. Framework-agnostic."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from databricks import sql
from databricks.sdk.core import Config as _SdkConfig

from .config import Settings, settings
from .exceptions import OmopConnectionError, OmopSqlError


def server_hostname(s: Settings | None = None) -> str:
    s = s or settings()
    host = s.host or _SdkConfig().host
    return host.replace("https://", "").rstrip("/")


@contextmanager
def cursor(token: str, s: Settings | None = None) -> Iterator[Any]:
    s = s or settings()
    with sql.connect(
        server_hostname=server_hostname(s),
        http_path=f"/sql/1.0/warehouses/{s.warehouse_id}",
        access_token=token,
    ) as conn, conn.cursor() as cur:
        yield cur


def query(
    stmt: str,
    params: dict[str, Any] | None,
    token: str,
    s: Settings | None = None,
) -> list[dict[str, Any]]:
    """Execute a parameterised SQL statement and return rows as dicts."""
    try:
        with cursor(token, s) as cur:
            cur.execute(stmt, params or {})
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    except sql.exc.ServerOperationError as exc:
        raise OmopSqlError(str(exc)) from exc
    except sql.exc.Error as exc:
        raise OmopConnectionError(str(exc)) from exc
