"""Gradio handler contract tests.

Focus: input validation, OBO token forwarding, pd.DataFrame shaping, and
OmopError → gr.Error translation. Query/diagnostics logic is covered in
`src/omop_lib/tests/`.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import gradio as gr
import pandas as pd
import pytest

from omop_lib.exceptions import OmopConnectionError, OmopNotFound, OmopSqlError


# ---------- concept search ----------

def test_concept_search_forwards_to_lib(app_module, fake_queries, fake_request):
    fake_queries.concept_search.return_value = [{"concept_id": 1}]
    out = app_module.do_concept_search("diabetes", "SNOMED", fake_request)
    assert isinstance(out, pd.DataFrame)
    fake_queries.concept_search.assert_called_once_with("diabetes", "SNOMED", "fake-token")


def test_concept_search_blank_vocab_becomes_none(app_module, fake_queries, fake_request):
    fake_queries.concept_search.return_value = []
    app_module.do_concept_search("diabetes", "", fake_request)
    fake_queries.concept_search.assert_called_once_with("diabetes", None, "fake-token")


def test_concept_search_too_short(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_concept_search("a", "", fake_request)


def test_concept_search_translates_sql_error(app_module, fake_queries, fake_request):
    fake_queries.concept_search.side_effect = OmopSqlError("UNRESOLVED_COLUMN")
    with pytest.raises(gr.Error, match="OmopSqlError"):
        app_module.do_concept_search("diabetes", "", fake_request)


# ---------- patient lookup ----------

def test_patient_lookup_happy(app_module, fake_queries, fake_request):
    fake_queries.patient_summary.return_value = {"person_id": 42, "gender": "F"}
    fake_queries.drug_timeline.return_value = [{"drug_name": "Metformin"}]
    summary, drugs = app_module.do_patient_lookup(42, fake_request)
    assert summary == {"person_id": 42, "gender": "F"}
    assert len(drugs) == 1
    fake_queries.patient_summary.assert_called_once_with(42, "fake-token")
    fake_queries.drug_timeline.assert_called_once_with(42, None, "fake-token")


def test_patient_lookup_coerces_float(app_module, fake_queries, fake_request):
    fake_queries.patient_summary.return_value = {"person_id": 42}
    fake_queries.drug_timeline.return_value = []
    app_module.do_patient_lookup(42.0, fake_request)
    fake_queries.patient_summary.assert_called_once_with(42, "fake-token")


def test_patient_lookup_not_found(app_module, fake_queries, fake_request):
    fake_queries.patient_summary.side_effect = OmopNotFound("person_id 99999 not found")
    with pytest.raises(gr.Error, match="OmopNotFound"):
        app_module.do_patient_lookup(99999, fake_request)


def test_patient_lookup_missing_pid(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_patient_lookup(None, fake_request)


# ---------- cohort sizer ----------

def test_cohort_size_formats_grouped(app_module, fake_queries, fake_request):
    fake_queries.condition_cohort_size.return_value = 12345
    md = app_module.do_cohort_size(4229440, fake_request)
    assert "12,345" in md
    assert "4229440" in md
    fake_queries.condition_cohort_size.assert_called_once_with(4229440, "fake-token")


def test_cohort_size_translates_error(app_module, fake_queries, fake_request):
    fake_queries.condition_cohort_size.side_effect = OmopConnectionError("socket closed")
    with pytest.raises(gr.Error, match="OmopConnectionError"):
        app_module.do_cohort_size(4229440, fake_request)


def test_cohort_size_missing_cid(app_module, fake_request):
    with pytest.raises(gr.Error):
        app_module.do_cohort_size(None, fake_request)


# ---------- top conditions ----------

def test_top_conditions_forwards_slider_int(app_module, fake_queries, fake_request):
    fake_queries.top_conditions.return_value = [{"condition_concept_id": 1}]
    app_module.do_top_conditions(25.0, fake_request)
    fake_queries.top_conditions.assert_called_once_with(25, "fake-token")


# ---------- whoami ----------

def test_whoami_local(app_module):
    req = MagicMock()
    req.headers = {}
    md = app_module.do_whoami(req)
    assert "local" in md
    assert "test_cat" in md


def test_whoami_obo(app_module):
    req = MagicMock()
    req.headers = {"x-forwarded-email": "alice@example.com"}
    md = app_module.do_whoami(req)
    assert "alice@example.com" in md
    assert "OBO" in md


# ---------- diagnostics ----------

def test_diagnostics_forwards_to_lib(app_module, fake_diagnostics, fake_request):
    probe = MagicMock()
    probe.as_dict.return_value = {"name": "probe", "status": "ok", "detail": "1ms"}
    fake_diagnostics.run.return_value = [probe]

    df = app_module.do_diagnostics(fake_request)
    assert list(df.columns) == ["status", "name", "detail"]
    assert len(df) == 1
    assert df.iloc[0]["name"] == "probe"
    fake_diagnostics.run.assert_called_once_with("fake-token")
