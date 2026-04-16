"""FastAPI endpoint contract tests.

Focus: route wiring, input validation, OBO header propagation, error
translation. SQL correctness is in `src/omop_lib/tests/test_queries.py`.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from omop_lib.exceptions import (
    OmopConnectionError,
    OmopNotFound,
    OmopSqlError,
)


# ---------- health + identity ----------

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "catalog": "test_cat",
        "schema": "test_schema",
        "warehouse_id": "test_wh_id",
    }


def test_whoami_local(client):
    body = client.get("/api/me").json()
    assert body["email"] is None
    assert body["auth_mode"] == "local"
    assert "checks" not in body


def test_whoami_obo_from_header(client):
    body = client.get("/api/me", headers={"X-Forwarded-Email": "a@x.com"}).json()
    assert body["email"] == "a@x.com"
    assert body["auth_mode"] == "obo"


def test_whoami_diagnostics_forwarded_to_lib(client, fake_diagnostics):
    probe = MagicMock()
    probe.as_dict.return_value = {"name": "probe", "status": "ok", "detail": ""}
    fake_diagnostics.run.return_value = [probe]

    body = client.get("/api/me?include_diagnostics=true").json()
    assert body["checks"] == [{"name": "probe", "status": "ok", "detail": ""}]
    fake_diagnostics.run.assert_called_once_with("fake-token")


def test_whoami_diagnostics_auth_failure(client, fake_diagnostics, app_module, monkeypatch):
    from fastapi import HTTPException as _HE

    def raise_auth(_hdr):
        raise _HE(status_code=401, detail="no creds")

    monkeypatch.setattr(app_module, "_resolve_token", raise_auth)

    body = client.get("/api/me?include_diagnostics=true").json()
    assert body["checks"] == [{"name": "Authentication", "status": "fail", "detail": "no creds"}]
    fake_diagnostics.run.assert_not_called()


# ---------- concept search ----------

def test_concept_search_forwards_to_lib(client, fake_queries):
    fake_queries.concept_search.return_value = [{"concept_id": 1}]
    resp = client.get("/api/concepts/search", params={"q": "diabetes", "vocab": "SNOMED"})
    assert resp.status_code == 200
    assert resp.json() == [{"concept_id": 1}]
    fake_queries.concept_search.assert_called_once_with("diabetes", "SNOMED", "fake-token")


def test_concept_search_rejects_short_keyword(client):
    assert client.get("/api/concepts/search", params={"q": "a"}).status_code == 422


def test_concept_search_rejects_oversize_limit(client):
    assert client.get("/api/concepts/search", params={"q": "diabetes", "limit": 500}).status_code == 422


def test_concept_search_limit_truncates_response(client, fake_queries):
    fake_queries.concept_search.return_value = [{"concept_id": i} for i in range(80)]
    resp = client.get("/api/concepts/search", params={"q": "diabetes", "limit": 5})
    assert len(resp.json()) == 5


# ---------- descendants / summary / drugs / cohort / top ----------

def test_concept_descendants(client, fake_queries):
    fake_queries.concept_descendants.return_value = [{"descendant_concept_id": 1}]
    client.get("/api/concepts/4229440/descendants")
    fake_queries.concept_descendants.assert_called_once_with(4229440, "fake-token")


def test_patient_summary_happy(client, fake_queries):
    fake_queries.patient_summary.return_value = {"person_id": 42}
    assert client.get("/api/patients/42/summary").json() == {"person_id": 42}


def test_patient_summary_translates_not_found(client, fake_queries):
    fake_queries.patient_summary.side_effect = OmopNotFound("person_id 99999 not found")
    resp = client.get("/api/patients/99999/summary")
    assert resp.status_code == 404
    assert "99999" in resp.json()["detail"]


def test_drug_timeline_forwards_filter(client, fake_queries):
    fake_queries.drug_timeline.return_value = []
    client.get("/api/patients/42/drugs", params={"drug_concept_id": 1124957})
    fake_queries.drug_timeline.assert_called_once_with(42, 1124957, "fake-token")


def test_drug_timeline_forwards_null_filter(client, fake_queries):
    fake_queries.drug_timeline.return_value = []
    client.get("/api/patients/42/drugs")
    fake_queries.drug_timeline.assert_called_once_with(42, None, "fake-token")


def test_drug_timeline_limit_truncates(client, fake_queries):
    fake_queries.drug_timeline.return_value = [{"drug_concept_id": i} for i in range(500)]
    resp = client.get("/api/patients/42/drugs", params={"limit": 10})
    assert len(resp.json()) == 10


def test_cohort_size(client, fake_queries):
    fake_queries.condition_cohort_size.return_value = 1234
    assert client.get("/api/cohorts/condition/4229440/size").json() == {
        "condition_concept_id": 4229440,
        "n_patients": 1234,
    }


def test_top_conditions(client, fake_queries):
    fake_queries.top_conditions.return_value = [{"condition_concept_id": 1}]
    resp = client.get("/api/conditions/top", params={"top_n": 5})
    assert resp.status_code == 200
    fake_queries.top_conditions.assert_called_once_with(5, "fake-token")


def test_top_conditions_default_is_20(client, fake_queries):
    fake_queries.top_conditions.return_value = []
    client.get("/api/conditions/top")
    fake_queries.top_conditions.assert_called_once_with(20, "fake-token")


def test_top_conditions_rejects_oob(client):
    assert client.get("/api/conditions/top", params={"top_n": 0}).status_code == 422
    assert client.get("/api/conditions/top", params={"top_n": 9999}).status_code == 422


# ---------- error translation ----------

def test_sql_error_becomes_400(client, fake_queries):
    fake_queries.top_conditions.side_effect = OmopSqlError("UNRESOLVED_COLUMN")
    resp = client.get("/api/conditions/top")
    assert resp.status_code == 400
    assert resp.json()["detail"] == {"sql_error": "UNRESOLVED_COLUMN"}


def test_connection_error_becomes_502(client, fake_queries):
    fake_queries.top_conditions.side_effect = OmopConnectionError("socket closed")
    resp = client.get("/api/conditions/top")
    assert resp.status_code == 502
    assert resp.json()["detail"] == {"warehouse_error": "socket closed"}


def test_unexpected_exception_becomes_500(client, fake_queries):
    fake_queries.top_conditions.side_effect = RuntimeError("kaboom")
    resp = client.get("/api/conditions/top")
    assert resp.status_code == 500
    assert resp.json() == {"detail": "Internal server error. Check app logs for trace."}


# ---------- static UI ----------

def test_root_serves_index_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "OMOP Explorer" in resp.text


def test_static_js_served(client):
    resp = client.get("/app.js")
    assert resp.status_code == 200
    assert "apiGet" in resp.text
