"""OMOP Explorer — Gradio flavor.

Same UC function surface as `src/app/` (the FastAPI flavor) but rendered with
Gradio Blocks for a lighter-touch UI that teams often prefer for internal
tools and demos.

Auth: user-on-behalf-of (OBO). Databricks Apps injects
`X-Forwarded-Access-Token` on every request; we read it from `gr.Request` and
hand it to `databricks-sql-connector` per call, so every query executes as
the real user (UC row filters + audit logs attribute correctly).
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

import gradio as gr
import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

logger = logging.getLogger("omop_explorer_gradio")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

CATALOG = os.environ["DATABRICKS_CATALOG"]
OMOP_SCHEMA = os.environ["DATABRICKS_OMOP_SCHEMA"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
FQN = f"`{CATALOG}`.`{OMOP_SCHEMA}`"
PORT = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))

OMOP_FUNCTIONS = (
    "omop_concept_search",
    "omop_concept_descendants",
    "omop_patient_summary",
    "omop_condition_cohort_size",
    "omop_drug_timeline",
    "omop_top_conditions",
)

_cfg = Config()


# ---------------------------------------------------------------------------
# Auth + warehouse
# ---------------------------------------------------------------------------
def _server_hostname() -> str:
    host = os.environ.get("DATABRICKS_HOST") or _cfg.host
    return host.replace("https://", "").rstrip("/")


def _token(request: gr.Request | None) -> str:
    if request is not None:
        hdr = request.headers.get("x-forwarded-access-token")
        if hdr:
            return hdr
    # Local dev fallback — SDK's auth chain
    auth = _cfg.authenticate().get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth.removeprefix("Bearer ").strip()
    raise gr.Error(
        "No authentication. Deploy as a Databricks App or set DATABRICKS_CONFIG_PROFILE / DATABRICKS_TOKEN locally."
    )


def _email(request: gr.Request | None) -> str:
    if request is None:
        return "local"
    return request.headers.get("x-forwarded-email") or "local"


@contextmanager
def _cursor(token: str) -> Iterator[Any]:
    with sql.connect(
        server_hostname=_server_hostname(),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    ) as conn, conn.cursor() as cur:
        yield cur


def _query(stmt: str, params: dict[str, Any], token: str) -> pd.DataFrame:
    try:
        with _cursor(token) as cur:
            cur.execute(stmt, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame([dict(zip(cols, r)) for r in cur.fetchall()])
    except sql.exc.ServerOperationError as exc:
        raise gr.Error(f"SQL error: {exc}") from exc
    except sql.exc.Error as exc:
        raise gr.Error(f"Warehouse error: {exc}") from exc


# ---------------------------------------------------------------------------
# Handlers — one per tab
# ---------------------------------------------------------------------------
def do_concept_search(q: str, vocab: str, request: gr.Request) -> pd.DataFrame:
    if not q or len(q) < 2:
        raise gr.Error("Enter at least 2 characters.")
    return _query(
        f"SELECT * FROM {FQN}.omop_concept_search(:q, :v) LIMIT 100",
        {"q": q, "v": vocab or None},
        _token(request),
    )


def do_patient_lookup(
    pid: int | float | None, request: gr.Request
) -> tuple[dict[str, Any], pd.DataFrame]:
    if pid is None:
        raise gr.Error("Enter a person_id.")
    pid_int = int(pid)
    token = _token(request)
    summary = _query(
        f"SELECT * FROM {FQN}.omop_patient_summary(:pid)",
        {"pid": pid_int},
        token,
    )
    if summary.empty:
        raise gr.Error(f"person_id {pid_int} not found.")
    drugs = _query(
        f"SELECT * FROM {FQN}.omop_drug_timeline(:pid, NULL) LIMIT 500",
        {"pid": pid_int},
        token,
    )
    return summary.iloc[0].to_dict(), drugs


def do_cohort_size(cid: int | float | None, request: gr.Request) -> str:
    if cid is None:
        raise gr.Error("Enter a condition_concept_id.")
    cid_int = int(cid)
    df = _query(
        f"SELECT {FQN}.omop_condition_cohort_size(:cid) AS n_patients",
        {"cid": cid_int},
        _token(request),
    )
    n = int(df.iloc[0]["n_patients"])
    return f"**{n:,}** patients with concept `{cid_int}` or any descendant."


def do_top_conditions(top_n: int, request: gr.Request) -> pd.DataFrame:
    return _query(
        f"SELECT * FROM {FQN}.omop_top_conditions(:n)",
        {"n": int(top_n)},
        _token(request),
    )


def do_diagnostics(request: gr.Request) -> pd.DataFrame:
    """Six user-scoped probes; mirror the FastAPI flavor so behavior stays consistent."""
    token = _token(request)
    rows: list[dict[str, Any]] = []

    def run(name: str, fn: Any) -> None:
        try:
            detail = fn() or ""
            rows.append({"status": "ok", "check": name, "detail": detail})
        except Exception as exc:
            rows.append(
                {"status": "fail", "check": name, "detail": f"{type(exc).__name__}: {exc}"[:400]}
            )

    def warehouse_state() -> str:
        w = WorkspaceClient(host=f"https://{_server_hostname()}", token=token)
        wh = w.warehouses.get(id=WAREHOUSE_ID)
        return f"state={wh.state}, size={wh.cluster_size or 'serverless'}"

    def select_one() -> str:
        t0 = time.perf_counter()
        df = _query("SELECT 1 AS ok", {}, token)
        dt = int((time.perf_counter() - t0) * 1000)
        if df.empty or int(df.iloc[0]["ok"]) != 1:
            raise RuntimeError("unexpected response")
        return f"{dt}ms round-trip"

    def catalog_use() -> str:
        df = _query(f"SHOW SCHEMAS IN `{CATALOG}`", {}, token)
        return f"{len(df)} schema(s) visible"

    def schema_use() -> str:
        df = _query(f"SHOW TABLES IN `{CATALOG}`.`{OMOP_SCHEMA}`", {}, token)
        return f"{len(df)} table(s) visible"

    def functions_visible() -> str:
        df = _query(
            f"SHOW USER FUNCTIONS IN `{CATALOG}`.`{OMOP_SCHEMA}` LIKE 'omop_*'",
            {},
            token,
        )
        found = {
            str(r).split(".")[-1]
            for r in (df.iloc[:, 0].tolist() if not df.empty else [])
        }
        missing = [f for f in OMOP_FUNCTIONS if f not in found]
        return (
            f"{len(OMOP_FUNCTIONS) - len(missing)}/{len(OMOP_FUNCTIONS)} visible — missing: {', '.join(missing)}"
            if missing
            else f"all {len(OMOP_FUNCTIONS)} functions visible"
        )

    def function_execute() -> str:
        df = _query(f"SELECT * FROM {FQN}.omop_top_conditions(:n)", {"n": 1}, token)
        return f"returned {len(df)} row(s)"

    run("Warehouse reachable", warehouse_state)
    run("SELECT 1 round-trip", select_one)
    run(f"USE CATALOG `{CATALOG}`", catalog_use)
    run(f"USE SCHEMA `{OMOP_SCHEMA}`", schema_use)
    run("OMOP functions visible", functions_visible)
    run("EXECUTE omop_top_conditions(1)", function_execute)
    return pd.DataFrame(rows, columns=["status", "check", "detail"])


def do_whoami(request: gr.Request) -> str:
    email = _email(request)
    mode = "OBO" if email != "local" else "local"
    return f"**{email}** · auth={mode} · catalog=`{CATALOG}` · schema=`{OMOP_SCHEMA}` · warehouse=`{WAREHOUSE_ID}`"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
with gr.Blocks(title="OMOP Explorer (Gradio)", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# OMOP Explorer")
    whoami_md = gr.Markdown()
    demo.load(do_whoami, outputs=whoami_md)

    with gr.Tabs():
        with gr.Tab("Concept search"):
            with gr.Row():
                q_in = gr.Textbox(label="Keyword", placeholder="e.g. diabetes", scale=2)
                vocab_in = gr.Textbox(
                    label="Vocabulary (optional)",
                    placeholder="SNOMED, LOINC, RxNorm …",
                    scale=1,
                )
            search_btn = gr.Button("Search", variant="primary")
            search_out = gr.Dataframe(label="Matches (top 100)", wrap=True)
            search_btn.click(
                do_concept_search, inputs=[q_in, vocab_in], outputs=search_out
            )

        with gr.Tab("Patient lookup"):
            pid_in = gr.Number(label="person_id", precision=0)
            lookup_btn = gr.Button("Load", variant="primary")
            summary_out = gr.JSON(label="Summary")
            drugs_out = gr.Dataframe(label="Drug timeline (latest 500)", wrap=True)
            lookup_btn.click(
                do_patient_lookup, inputs=pid_in, outputs=[summary_out, drugs_out]
            )

        with gr.Tab("Cohort sizer"):
            gr.Markdown(
                "Counts distinct patients with the given condition concept id "
                "**or any of its descendants**."
            )
            cid_in = gr.Number(label="condition_concept_id", precision=0)
            cohort_btn = gr.Button("Size cohort", variant="primary")
            cohort_out = gr.Markdown()
            cohort_btn.click(do_cohort_size, inputs=cid_in, outputs=cohort_out)

        with gr.Tab("Top conditions"):
            top_n_in = gr.Slider(
                label="Top N", minimum=5, maximum=200, value=20, step=5
            )
            top_btn = gr.Button("Load", variant="primary")
            top_out = gr.Dataframe(label="Most prevalent conditions", wrap=True)
            top_btn.click(do_top_conditions, inputs=top_n_in, outputs=top_out)

        with gr.Tab("Diagnostics"):
            gr.Markdown(
                "User-scoped probes against UC + the warehouse. "
                "Runs as **you** (OBO) — failures here show perm gaps you hit, not the app SP."
            )
            diag_btn = gr.Button("Run diagnostics", variant="primary")
            diag_out = gr.Dataframe(
                label="Checks",
                headers=["status", "check", "detail"],
                wrap=True,
            )
            diag_btn.click(do_diagnostics, outputs=diag_out)


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=PORT)
