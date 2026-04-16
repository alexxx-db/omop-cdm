"""OMOP Explorer — Gradio flavor.

Same UC function surface as the FastAPI flavor; queries + diagnostics are
delegated to `omop_lib`. Framework-specific concerns (OBO token extraction
from `gr.Request`, `gr.Error` translation) live here.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import gradio as gr
import pandas as pd
from databricks.sdk.core import Config

from omop_lib import diagnostics, queries
from omop_lib.config import settings
from omop_lib.exceptions import OmopError

logger = logging.getLogger("omop_explorer_gradio")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

_S = settings()
PORT = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))

_cfg = Config()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def _token(request: gr.Request | None) -> str:
    if request is not None:
        hdr = request.headers.get("x-forwarded-access-token")
        if hdr:
            return hdr
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


# ---------------------------------------------------------------------------
# Handlers — translate OmopError into gr.Error, shape into Gradio components
# ---------------------------------------------------------------------------
def _raise_gr(exc: OmopError) -> None:
    raise gr.Error(f"{type(exc).__name__}: {exc}") from exc


def do_concept_search(q: str, vocab: str, request: gr.Request) -> pd.DataFrame:
    if not q or len(q) < 2:
        raise gr.Error("Enter at least 2 characters.")
    try:
        rows = queries.concept_search(q, vocab or None, _token(request))
    except OmopError as exc:
        _raise_gr(exc)
    return pd.DataFrame(rows[:100])


def do_patient_lookup(
    pid: int | float | None, request: gr.Request
) -> tuple[dict[str, Any], pd.DataFrame]:
    if pid is None:
        raise gr.Error("Enter a person_id.")
    pid_int = int(pid)
    token = _token(request)
    try:
        summary = queries.patient_summary(pid_int, token)
        drugs = queries.drug_timeline(pid_int, None, token)
    except OmopError as exc:
        _raise_gr(exc)
    return summary, pd.DataFrame(drugs[:500])


def do_cohort_size(cid: int | float | None, request: gr.Request) -> str:
    if cid is None:
        raise gr.Error("Enter a condition_concept_id.")
    cid_int = int(cid)
    try:
        n = queries.condition_cohort_size(cid_int, _token(request))
    except OmopError as exc:
        _raise_gr(exc)
    return f"**{n:,}** patients with concept `{cid_int}` or any descendant."


def do_top_conditions(top_n: int, request: gr.Request) -> pd.DataFrame:
    try:
        rows = queries.top_conditions(int(top_n), _token(request))
    except OmopError as exc:
        _raise_gr(exc)
    return pd.DataFrame(rows)


def do_diagnostics(request: gr.Request) -> pd.DataFrame:
    checks = diagnostics.run(_token(request))
    return pd.DataFrame([c.as_dict() for c in checks])[["status", "name", "detail"]]


def do_whoami(request: gr.Request) -> str:
    email = _email(request)
    mode = "OBO" if email != "local" else "local"
    return (
        f"**{email}** · auth={mode} · catalog=`{_S.catalog}` · "
        f"schema=`{_S.omop_schema}` · warehouse=`{_S.warehouse_id}`"
    )


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
            search_btn.click(do_concept_search, inputs=[q_in, vocab_in], outputs=search_out)

        with gr.Tab("Patient lookup"):
            pid_in = gr.Number(label="person_id", precision=0)
            lookup_btn = gr.Button("Load", variant="primary")
            summary_out = gr.JSON(label="Summary")
            drugs_out = gr.Dataframe(label="Drug timeline (latest 500)", wrap=True)
            lookup_btn.click(do_patient_lookup, inputs=pid_in, outputs=[summary_out, drugs_out])

        with gr.Tab("Cohort sizer"):
            gr.Markdown("Counts distinct patients with the given condition concept id **or any descendant**.")
            cid_in = gr.Number(label="condition_concept_id", precision=0)
            cohort_btn = gr.Button("Size cohort", variant="primary")
            cohort_out = gr.Markdown()
            cohort_btn.click(do_cohort_size, inputs=cid_in, outputs=cohort_out)

        with gr.Tab("Top conditions"):
            top_n_in = gr.Slider(label="Top N", minimum=5, maximum=200, value=20, step=5)
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
                headers=["status", "name", "detail"],
                wrap=True,
            )
            diag_btn.click(do_diagnostics, outputs=diag_out)


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=PORT)
