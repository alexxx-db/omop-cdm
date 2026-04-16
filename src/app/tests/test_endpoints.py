"""Endpoint smoke tests for OMOP Explorer.

These verify the HTTP contract — status codes, response shape, input
validation, and that the right parameters are forwarded to the SQL layer.
Real warehouse execution is not exercised; see integration tests for that.
"""

from __future__ import annotations

from fastapi import HTTPException


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
    resp = client.get("/api/me")
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] is None
    assert body["auth_mode"] == "local"
    assert body["catalog"] == "test_cat"
    assert body["schema"] == "test_schema"
    assert body["warehouse_id"] == "test_wh_id"
    assert "checks" not in body


def test_whoami_obo_from_header(client):
    resp = client.get("/api/me", headers={"X-Forwarded-Email": "alice@example.com"})
    body = resp.json()
    assert body["email"] == "alice@example.com"
    assert body["auth_mode"] == "obo"


def test_whoami_diagnostics_all_ok(client, fake_run, monkeypatch, app_module):
    # _run is called 5 times in _diagnostics (SELECT 1, SHOW SCHEMAS, SHOW TABLES,
    # SHOW USER FUNCTIONS, omop_top_conditions). The 6th check uses the SDK.
    from unittest.mock import MagicMock

    fake_run.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "bronze"}, {"databaseName": "omop531"}],
        [{"tableName": "person"}, {"tableName": "concept"}],
        [{"function": f"test_cat.test_schema.{f}"} for f in app_module.OMOP_FUNCTIONS],
        [{"condition_concept_id": 1, "n_patients": 10}],
    ]
    mock_wh = MagicMock()
    mock_wh.state = "RUNNING"
    mock_wh.cluster_size = "2X-Small"
    mock_ws = MagicMock()
    mock_ws.warehouses.get.return_value = mock_wh
    monkeypatch.setattr(app_module, "WorkspaceClient", lambda **_: mock_ws)

    resp = client.get("/api/me?include_diagnostics=true")
    assert resp.status_code == 200
    checks = resp.json()["checks"]
    assert len(checks) == 6
    statuses = [c["status"] for c in checks]
    assert statuses == ["ok"] * 6
    # Spot-check content
    assert any("RUNNING" in c["detail"] for c in checks)
    assert any("round-trip" in c["detail"] for c in checks)
    assert any("all 6 functions" in c["detail"] for c in checks)


def test_whoami_diagnostics_missing_functions(client, fake_run, monkeypatch, app_module):
    from unittest.mock import MagicMock

    # Everything before the SHOW FUNCTIONS call succeeds; then return a partial list.
    fake_run.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        [{"function": "test_cat.test_schema.omop_concept_search"}],  # only 1 of 6
        [{"condition_concept_id": 1, "n_patients": 10}],
    ]
    mock_ws = MagicMock()
    mock_ws.warehouses.get.return_value = MagicMock(state="RUNNING", cluster_size=None)
    monkeypatch.setattr(app_module, "WorkspaceClient", lambda **_: mock_ws)

    resp = client.get("/api/me?include_diagnostics=true")
    checks = resp.json()["checks"]
    fn_check = next(c for c in checks if c["name"] == "OMOP functions visible")
    assert fn_check["status"] == "ok"  # the probe itself ran; the detail reports the gap
    assert "missing" in fn_check["detail"]
    assert "omop_top_conditions" in fn_check["detail"]


def test_whoami_diagnostics_warehouse_failure(client, fake_run, monkeypatch, app_module):
    from unittest.mock import MagicMock

    mock_ws = MagicMock()
    mock_ws.warehouses.get.side_effect = PermissionError("no access")
    monkeypatch.setattr(app_module, "WorkspaceClient", lambda **_: mock_ws)

    # Remaining _run-based checks succeed with minimal shapes
    fake_run.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        [{"function": f"test_cat.test_schema.{f}"} for f in app_module.OMOP_FUNCTIONS],
        [{"condition_concept_id": 1}],
    ]

    resp = client.get("/api/me?include_diagnostics=true")
    checks = resp.json()["checks"]
    wh_check = next(c for c in checks if c["name"] == "Warehouse reachable")
    assert wh_check["status"] == "fail"
    assert "PermissionError" in wh_check["detail"]


def test_whoami_diagnostics_auth_failure_returns_single_check(client, app_module, monkeypatch):
    # Strip the default OBO bypass so _resolve_token raises 401
    from fastapi import HTTPException as _HE

    def raise_auth(_hdr):
        raise _HE(status_code=401, detail="no creds")

    monkeypatch.setattr(app_module, "_resolve_token", raise_auth)

    resp = client.get("/api/me?include_diagnostics=true")
    assert resp.status_code == 200  # endpoint still responds; failure is in the body
    body = resp.json()
    assert body["checks"] == [{"name": "Authentication", "status": "fail", "detail": "no creds"}]


# ---------- concept search ----------

def test_concept_search_happy_path(client, fake_run):
    fake_run.return_value = [
        {
            "concept_id": 201826,
            "concept_name": "Type 2 diabetes mellitus",
            "vocabulary_id": "SNOMED",
            "domain_id": "Condition",
            "concept_class_id": "Clinical Finding",
            "standard_concept": "S",
        }
    ]
    resp = client.get("/api/concepts/search", params={"q": "diabetes", "vocab": "SNOMED"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["concept_name"].startswith("Type 2 diabetes")
    # params forwarded verbatim
    stmt, params, token = fake_run.call_args.args
    assert params == {"keyword": "diabetes", "vocab": "SNOMED"}
    assert token == "fake-token"
    assert "omop_concept_search" in stmt


def test_concept_search_rejects_short_keyword(client):
    resp = client.get("/api/concepts/search", params={"q": "a"})
    assert resp.status_code == 422


def test_concept_search_rejects_oversize_limit(client):
    resp = client.get("/api/concepts/search", params={"q": "diabetes", "limit": 500})
    assert resp.status_code == 422


def test_concept_search_client_side_limit_truncates(client, fake_run):
    fake_run.return_value = [{"concept_id": i} for i in range(80)]
    resp = client.get("/api/concepts/search", params={"q": "diabetes", "limit": 5})
    assert resp.status_code == 200
    assert len(resp.json()) == 5


def test_concept_search_null_vocab_forwarded_as_none(client, fake_run):
    client.get("/api/concepts/search", params={"q": "diabetes"})
    _, params, _ = fake_run.call_args.args
    assert params["vocab"] is None


# ---------- descendants ----------

def test_concept_descendants(client, fake_run):
    fake_run.return_value = [
        {"descendant_concept_id": 1, "descendant_name": "X", "min_levels_of_separation": 0, "max_levels_of_separation": 0},
    ]
    resp = client.get("/api/concepts/4229440/descendants")
    assert resp.status_code == 200
    assert fake_run.call_args.args[1] == {"cid": 4229440}


# ---------- patient summary ----------

def test_patient_summary_happy(client, fake_run):
    fake_run.return_value = [
        {"person_id": 42, "gender": "FEMALE", "age_years": 55, "n_visits": 3}
    ]
    resp = client.get("/api/patients/42/summary")
    assert resp.status_code == 200
    assert resp.json()["person_id"] == 42


def test_patient_summary_not_found(client, fake_run):
    fake_run.return_value = []
    resp = client.get("/api/patients/99999/summary")
    assert resp.status_code == 404
    assert "99999" in resp.json()["detail"]


# ---------- drug timeline ----------

def test_drug_timeline_with_filter(client, fake_run):
    fake_run.return_value = []
    client.get("/api/patients/42/drugs", params={"drug_concept_id": 1124957})
    assert fake_run.call_args.args[1] == {"pid": 42, "did": 1124957}


def test_drug_timeline_without_filter_passes_none(client, fake_run):
    fake_run.return_value = []
    client.get("/api/patients/42/drugs")
    assert fake_run.call_args.args[1] == {"pid": 42, "did": None}


def test_drug_timeline_limit_clamped(client, fake_run):
    fake_run.return_value = [{"drug_concept_id": i} for i in range(500)]
    resp = client.get("/api/patients/42/drugs", params={"limit": 10})
    assert len(resp.json()) == 10


# ---------- cohort size ----------

def test_cohort_size_shape(client, fake_run):
    fake_run.return_value = [{"n_patients": 1234}]
    resp = client.get("/api/cohorts/condition/4229440/size")
    assert resp.status_code == 200
    assert resp.json() == {"condition_concept_id": 4229440, "n_patients": 1234}


# ---------- top conditions ----------

def test_top_conditions_happy(client, fake_run):
    fake_run.return_value = [
        {"condition_concept_id": 1, "condition_name": "A", "n_patients": 10, "n_occurrences": 20},
    ]
    resp = client.get("/api/conditions/top", params={"top_n": 5})
    assert resp.status_code == 200
    assert fake_run.call_args.args[1] == {"n": 5}


def test_top_conditions_invalid_top_n(client):
    assert client.get("/api/conditions/top", params={"top_n": 0}).status_code == 422
    assert client.get("/api/conditions/top", params={"top_n": 9999}).status_code == 422


def test_top_conditions_default_top_n_is_20(client, fake_run):
    fake_run.return_value = []
    client.get("/api/conditions/top")
    assert fake_run.call_args.args[1] == {"n": 20}


# ---------- error propagation ----------

def test_sql_error_becomes_clean_400(client, fake_run):
    fake_run.side_effect = HTTPException(
        status_code=400, detail={"sql_error": "UNRESOLVED_COLUMN"}
    )
    resp = client.get("/api/conditions/top")
    assert resp.status_code == 400
    assert resp.json()["detail"] == {"sql_error": "UNRESOLVED_COLUMN"}


def test_unexpected_exception_becomes_500(client, fake_run):
    fake_run.side_effect = RuntimeError("kaboom")
    resp = client.get("/api/conditions/top", params={"top_n": 5})
    assert resp.status_code == 500
    assert resp.json() == {"detail": "Internal server error. Check app logs for trace."}


# ---------- static UI ----------

def test_root_serves_index_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "OMOP Explorer" in resp.text
    assert resp.headers["content-type"].startswith("text/html")


def test_static_js_served(client):
    resp = client.get("/app.js")
    assert resp.status_code == 200
    assert "apiGet" in resp.text
