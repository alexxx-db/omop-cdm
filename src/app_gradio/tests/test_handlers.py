"""Unit tests for the Gradio `do_*` handlers.

These exercise the handlers directly (no Blocks/UI wiring, no Gradio server,
no warehouse). Intent: lock the SQL-param surface, the input validation, the
gr.Error paths, and the diagnostics probe logic.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import gradio as gr
import pandas as pd
import pytest


# ---------- concept search ----------

def test_concept_search_happy(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame(
        [{"concept_id": 201826, "concept_name": "Type 2 diabetes mellitus", "vocabulary_id": "SNOMED"}]
    )
    out = app_module.do_concept_search("diabetes", "SNOMED", fake_request)
    assert isinstance(out, pd.DataFrame)
    assert len(out) == 1

    stmt, params, token = fake_query.call_args.args
    assert "omop_concept_search" in stmt
    assert params == {"q": "diabetes", "v": "SNOMED"}
    assert token == "fake-token"


def test_concept_search_blank_vocab_becomes_none(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame()
    app_module.do_concept_search("diabetes", "", fake_request)
    _, params, _ = fake_query.call_args.args
    assert params["v"] is None


def test_concept_search_too_short(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_concept_search("a", "", fake_request)


def test_concept_search_empty_keyword(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_concept_search("", "SNOMED", fake_request)


# ---------- patient lookup ----------

def test_patient_lookup_happy(app_module, fake_query, fake_request):
    fake_query.side_effect = [
        pd.DataFrame([{"person_id": 42, "gender": "FEMALE", "age_years": 55}]),
        pd.DataFrame([{"drug_era_start_date": "2020-01-01", "drug_name": "Metformin"}]),
    ]
    summary, drugs = app_module.do_patient_lookup(42, fake_request)
    assert summary["person_id"] == 42
    assert summary["gender"] == "FEMALE"
    assert len(drugs) == 1
    assert fake_query.call_count == 2

    # First call is the summary, second is the drug timeline
    summary_stmt, summary_params, _ = fake_query.call_args_list[0].args
    drugs_stmt, drugs_params, _ = fake_query.call_args_list[1].args
    assert "omop_patient_summary" in summary_stmt
    assert summary_params == {"pid": 42}
    assert "omop_drug_timeline" in drugs_stmt
    assert drugs_params == {"pid": 42}


def test_patient_lookup_coerces_float_to_int(app_module, fake_query, fake_request):
    """gr.Number returns floats by default; handler must coerce."""
    fake_query.side_effect = [
        pd.DataFrame([{"person_id": 42}]),
        pd.DataFrame(),
    ]
    app_module.do_patient_lookup(42.0, fake_request)
    _, params, _ = fake_query.call_args_list[0].args
    assert params == {"pid": 42}
    assert isinstance(params["pid"], int)


def test_patient_lookup_not_found(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame()
    with pytest.raises(gr.Error, match="not found"):
        app_module.do_patient_lookup(99999, fake_request)


def test_patient_lookup_missing_pid(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_patient_lookup(None, fake_request)


# ---------- cohort sizer ----------

def test_cohort_size_happy(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame([{"n_patients": 12345}])
    md = app_module.do_cohort_size(4229440, fake_request)
    assert "12,345" in md   # grouped thousands
    assert "4229440" in md  # echoes the concept id


def test_cohort_size_missing_cid(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_cohort_size(None, fake_request)


# ---------- top conditions ----------

def test_top_conditions_happy(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame(
        [{"condition_concept_id": 1, "condition_name": "A", "n_patients": 10, "n_occurrences": 20}]
    )
    out = app_module.do_top_conditions(10, fake_request)
    assert len(out) == 1
    _, params, _ = fake_query.call_args.args
    assert params == {"n": 10}


def test_top_conditions_coerces_slider_value(app_module, fake_query, fake_request):
    fake_query.return_value = pd.DataFrame()
    app_module.do_top_conditions(25.0, fake_request)
    _, params, _ = fake_query.call_args.args
    assert params == {"n": 25}
    assert isinstance(params["n"], int)


# ---------- whoami ----------

def test_whoami_local(app_module):
    req = MagicMock()
    req.headers = {}
    md = app_module.do_whoami(req)
    assert "local" in md
    assert "test_cat" in md
    assert "test_schema" in md


def test_whoami_obo(app_module):
    req = MagicMock()
    req.headers = {"x-forwarded-email": "alice@example.com"}
    md = app_module.do_whoami(req)
    assert "alice@example.com" in md
    assert "OBO" in md


# ---------- diagnostics ----------

def _stub_warehouse(app_module, monkeypatch, state: str = "RUNNING", size=None):
    mock_wh = MagicMock()
    mock_wh.state = state
    mock_wh.cluster_size = size
    mock_ws = MagicMock()
    mock_ws.warehouses.get.return_value = mock_wh
    monkeypatch.setattr(app_module, "WorkspaceClient", lambda **_: mock_ws)


def _all_functions_visible(app_module):
    return pd.DataFrame(
        [{"function": f"test_cat.test_schema.{f}"} for f in app_module.OMOP_FUNCTIONS]
    )


def test_diagnostics_all_ok(app_module, fake_query, fake_request, monkeypatch):
    fake_query.side_effect = [
        pd.DataFrame([{"ok": 1}]),                         # SELECT 1
        pd.DataFrame([{"databaseName": "omop531"}]),        # SHOW SCHEMAS
        pd.DataFrame([{"tableName": "person"}]),            # SHOW TABLES
        _all_functions_visible(app_module),                 # SHOW USER FUNCTIONS
        pd.DataFrame([{"condition_concept_id": 1}]),        # omop_top_conditions(1)
    ]
    _stub_warehouse(app_module, monkeypatch)

    df = app_module.do_diagnostics(fake_request)
    assert list(df.columns) == ["status", "check", "detail"]
    assert len(df) == 6
    assert (df["status"] == "ok").all()

    # Spot-check specific probe details
    detail_for = dict(zip(df["check"], df["detail"]))
    assert "RUNNING" in detail_for["Warehouse reachable"]
    assert "round-trip" in detail_for["SELECT 1 round-trip"]
    assert f"all {len(app_module.OMOP_FUNCTIONS)} functions visible" in detail_for["OMOP functions visible"]


def test_diagnostics_warehouse_permission_denied(app_module, fake_query, fake_request, monkeypatch):
    # Warehouse SDK call fails; remaining probes still run
    fake_query.side_effect = [
        pd.DataFrame([{"ok": 1}]),
        pd.DataFrame([{"databaseName": "omop531"}]),
        pd.DataFrame([{"tableName": "person"}]),
        _all_functions_visible(app_module),
        pd.DataFrame([{"condition_concept_id": 1}]),
    ]
    mock_ws = MagicMock()
    mock_ws.warehouses.get.side_effect = PermissionError("no access")
    monkeypatch.setattr(app_module, "WorkspaceClient", lambda **_: mock_ws)

    df = app_module.do_diagnostics(fake_request)
    wh_row = df[df["check"] == "Warehouse reachable"].iloc[0]
    assert wh_row["status"] == "fail"
    assert "PermissionError" in wh_row["detail"]
    # The other 5 still pass
    assert (df[df["check"] != "Warehouse reachable"]["status"] == "ok").all()


def test_diagnostics_missing_functions(app_module, fake_query, fake_request, monkeypatch):
    # Only one of six functions appears in SHOW USER FUNCTIONS
    fake_query.side_effect = [
        pd.DataFrame([{"ok": 1}]),
        pd.DataFrame([{"databaseName": "omop531"}]),
        pd.DataFrame([{"tableName": "person"}]),
        pd.DataFrame([{"function": "test_cat.test_schema.omop_concept_search"}]),
        pd.DataFrame([{"condition_concept_id": 1}]),
    ]
    _stub_warehouse(app_module, monkeypatch)

    df = app_module.do_diagnostics(fake_request)
    fn_row = df[df["check"] == "OMOP functions visible"].iloc[0]
    assert fn_row["status"] == "ok"  # probe itself succeeded; detail reports the gap
    assert "missing" in fn_row["detail"]
    assert "omop_top_conditions" in fn_row["detail"]


def test_diagnostics_select_one_returns_unexpected_shape(app_module, fake_query, fake_request, monkeypatch):
    fake_query.side_effect = [
        pd.DataFrame([{"ok": 0}]),  # wrong value → probe raises
        pd.DataFrame([{"databaseName": "omop531"}]),
        pd.DataFrame([{"tableName": "person"}]),
        _all_functions_visible(app_module),
        pd.DataFrame([{"condition_concept_id": 1}]),
    ]
    _stub_warehouse(app_module, monkeypatch)

    df = app_module.do_diagnostics(fake_request)
    sel_row = df[df["check"] == "SELECT 1 round-trip"].iloc[0]
    assert sel_row["status"] == "fail"
    assert "RuntimeError" in sel_row["detail"]
