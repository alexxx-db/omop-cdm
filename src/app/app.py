"""OMOP Explorer — FastAPI app served as a Databricks App.

Auth model: user-on-behalf-of (OBO).
  Databricks Apps injects the caller's OAuth token as `X-Forwarded-Access-Token`
  on every request. We propagate that token to the SQL warehouse, so queries
  execute as the end user — UC row-level filters and audit logs attribute to
  the real caller, not the app service principal.

Local dev falls back to the SDK's default auth chain (`~/.databrickscfg` or
`DATABRICKS_*` env vars), which is fine because only developers hit the app
locally.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("omop_explorer")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

CATALOG = os.environ["DATABRICKS_CATALOG"]
OMOP_SCHEMA = os.environ["DATABRICKS_OMOP_SCHEMA"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
FQN = f"`{CATALOG}`.`{OMOP_SCHEMA}`"

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(
    title="OMOP Explorer",
    description="Thin UI over the OMOP UC functions. Auth: OBO.",
    version="0.2.0",
)


# ---------------------------------------------------------------------------
# Auth + warehouse connection
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _sdk_config() -> Config:
    """SDK config — only used locally when no OBO token is on the request."""
    return Config()


def _server_hostname() -> str:
    # DATABRICKS_HOST is set by the Apps runtime; locally it comes from the SDK config.
    host = os.environ.get("DATABRICKS_HOST") or _sdk_config().host
    return host.replace("https://", "").rstrip("/")


def _resolve_token(x_forwarded_access_token: str | None) -> str:
    """Prefer the end-user token from Databricks Apps. Fall back to the
    developer's own token when running locally."""
    if x_forwarded_access_token:
        return x_forwarded_access_token
    # Local dev: SDK picks up profile / env credentials
    headers = _sdk_config().authenticate()
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="No OBO token on request and no local SDK credentials available.",
        )
    return auth.removeprefix("Bearer ").strip()


@contextmanager
def _cursor(token: str) -> Iterator[Any]:
    with sql.connect(
        server_hostname=_server_hostname(),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    ) as conn, conn.cursor() as cur:
        yield cur


def _rows(cur: Any) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _run(
    stmt: str,
    params: dict[str, Any],
    token: str,
) -> list[dict[str, Any]]:
    try:
        with _cursor(token) as cur:
            cur.execute(stmt, params)
            return _rows(cur)
    except sql.exc.ServerOperationError as exc:
        logger.warning("SQL error: %s", exc)
        raise HTTPException(status_code=400, detail={"sql_error": str(exc)}) from exc
    except sql.exc.Error as exc:
        logger.exception("Warehouse connection error")
        raise HTTPException(status_code=502, detail={"warehouse_error": str(exc)}) from exc


# ---------------------------------------------------------------------------
# Global error handler — always JSON, never leak stack traces
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Check app logs for trace."},
    )


# ---------------------------------------------------------------------------
# Endpoints — one per UC function
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "catalog": CATALOG,
        "schema": OMOP_SCHEMA,
        "warehouse_id": WAREHOUSE_ID,
    }


@app.get("/api/me")
def whoami(x_forwarded_email: str | None = Header(default=None)) -> dict[str, Any]:
    """Databricks Apps populates X-Forwarded-* headers with the caller's identity."""
    return {
        "email": x_forwarded_email,
        "auth_mode": "obo" if x_forwarded_email else "local",
    }


@app.get("/api/concepts/search")
def concept_search(
    q: str = Query(..., min_length=2, max_length=120, description="Keyword"),
    vocab: str | None = Query(None, max_length=40),
    limit: int = Query(50, ge=1, le=100),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    rows = _run(
        f"SELECT * FROM {FQN}.omop_concept_search(:keyword, :vocab)",
        {"keyword": q, "vocab": vocab},
        token,
    )
    return rows[:limit]


@app.get("/api/concepts/{concept_id}/descendants")
def concept_descendants(
    concept_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    return _run(
        f"SELECT * FROM {FQN}.omop_concept_descendants(:cid) ORDER BY min_levels_of_separation",
        {"cid": concept_id},
        token,
    )


@app.get("/api/patients/{person_id}/summary")
def patient_summary(
    person_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    token = _resolve_token(x_forwarded_access_token)
    rows = _run(
        f"SELECT * FROM {FQN}.omop_patient_summary(:pid)",
        {"pid": person_id},
        token,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"person_id {person_id} not found")
    return rows[0]


@app.get("/api/patients/{person_id}/drugs")
def drug_timeline(
    person_id: int,
    drug_concept_id: int | None = Query(None, description="Optional ancestor drug concept filter"),
    limit: int = Query(200, ge=1, le=2000),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    rows = _run(
        f"SELECT * FROM {FQN}.omop_drug_timeline(:pid, :did)",
        {"pid": person_id, "did": drug_concept_id},
        token,
    )
    return rows[:limit]


@app.get("/api/cohorts/condition/{condition_concept_id}/size")
def cohort_size(
    condition_concept_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    token = _resolve_token(x_forwarded_access_token)
    rows = _run(
        f"SELECT {FQN}.omop_condition_cohort_size(:cid) AS n_patients",
        {"cid": condition_concept_id},
        token,
    )
    return {"condition_concept_id": condition_concept_id, "n_patients": rows[0]["n_patients"]}


@app.get("/api/conditions/top")
def top_conditions(
    top_n: int = Query(20, ge=1, le=200),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    return _run(
        f"SELECT * FROM {FQN}.omop_top_conditions(:n)",
        {"n": top_n},
        token,
    )


# ---------------------------------------------------------------------------
# Static UI (mounted last so /api/* wins routing)
# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
