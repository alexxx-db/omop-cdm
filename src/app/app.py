"""OMOP Explorer — FastAPI flavor.

Auth model: user-on-behalf-of (OBO). The end-user token comes via the
`X-Forwarded-Access-Token` header (Databricks Apps injects it on every
request). Queries + diagnostics delegate to `omop_lib`.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from databricks.sdk.core import Config
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from omop_lib import diagnostics, queries
from omop_lib.config import settings
from omop_lib.exceptions import (
    OmopConnectionError,
    OmopError,
    OmopNotFound,
    OmopSqlError,
)

logger = logging.getLogger("omop_explorer")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

_S = settings()
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(
    title="OMOP Explorer",
    description="FastAPI UI over the OMOP UC functions. Auth: OBO.",
    version="0.3.0",
)


# ---------------------------------------------------------------------------
# Token resolution — framework-specific, stays here
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _sdk_config() -> Config:
    return Config()


def _resolve_token(x_forwarded_access_token: str | None) -> str:
    if x_forwarded_access_token:
        return x_forwarded_access_token
    headers = _sdk_config().authenticate()
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="No OBO token on request and no local SDK credentials available.",
        )
    return auth.removeprefix("Bearer ").strip()


# ---------------------------------------------------------------------------
# Error translation — OmopError subclass → HTTP status
# ---------------------------------------------------------------------------
def _http_from_omop(exc: OmopError) -> HTTPException:
    if isinstance(exc, OmopNotFound):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, OmopSqlError):
        return HTTPException(status_code=400, detail={"sql_error": str(exc)})
    if isinstance(exc, OmopConnectionError):
        return HTTPException(status_code=502, detail={"warehouse_error": str(exc)})
    return HTTPException(status_code=500, detail=str(exc))


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Check app logs for trace."},
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "catalog": _S.catalog,
        "schema": _S.omop_schema,
        "warehouse_id": _S.warehouse_id,
    }


@app.get("/api/me")
def whoami(
    x_forwarded_email: str | None = Header(default=None),
    x_forwarded_access_token: str | None = Header(default=None),
    include_diagnostics: bool = Query(False, description="Run live probes (1-3s)."),
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "email": x_forwarded_email,
        "auth_mode": "obo" if x_forwarded_email else "local",
        "catalog": _S.catalog,
        "schema": _S.omop_schema,
        "warehouse_id": _S.warehouse_id,
    }
    if not include_diagnostics:
        return result
    try:
        token = _resolve_token(x_forwarded_access_token)
    except HTTPException as exc:
        result["checks"] = [
            {"name": "Authentication", "status": "fail", "detail": str(exc.detail)}
        ]
        return result
    result["checks"] = [c.as_dict() for c in diagnostics.run(token)]
    return result


@app.get("/api/concepts/search")
def concept_search(
    q: str = Query(..., min_length=2, max_length=120),
    vocab: str | None = Query(None, max_length=40),
    limit: int = Query(50, ge=1, le=100),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        rows = queries.concept_search(q, vocab, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc
    return rows[:limit]


@app.get("/api/concepts/{concept_id}/descendants")
def concept_descendants(
    concept_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        return queries.concept_descendants(concept_id, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc


@app.get("/api/patients/{person_id}/summary")
def patient_summary(
    person_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        return queries.patient_summary(person_id, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc


@app.get("/api/patients/{person_id}/drugs")
def drug_timeline(
    person_id: int,
    drug_concept_id: int | None = Query(None),
    limit: int = Query(200, ge=1, le=2000),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        rows = queries.drug_timeline(person_id, drug_concept_id, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc
    return rows[:limit]


@app.get("/api/cohorts/condition/{condition_concept_id}/size")
def cohort_size(
    condition_concept_id: int,
    x_forwarded_access_token: str | None = Header(default=None),
) -> dict[str, Any]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        n = queries.condition_cohort_size(condition_concept_id, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc
    return {"condition_concept_id": condition_concept_id, "n_patients": n}


@app.get("/api/conditions/top")
def top_conditions(
    top_n: int = Query(20, ge=1, le=200),
    x_forwarded_access_token: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    token = _resolve_token(x_forwarded_access_token)
    try:
        return queries.top_conditions(top_n, token)
    except OmopError as exc:
        raise _http_from_omop(exc) from exc


# Static UI mounted last so /api/* wins routing
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
