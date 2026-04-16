from __future__ import annotations

from unittest.mock import MagicMock

from omop_lib import diagnostics, queries


def _stub_warehouse(monkeypatch, *, state="RUNNING", size=None, side_effect=None):
    mock_wh = MagicMock()
    mock_wh.state = state
    mock_wh.cluster_size = size
    mock_ws = MagicMock()
    if side_effect is not None:
        mock_ws.warehouses.get.side_effect = side_effect
    else:
        mock_ws.warehouses.get.return_value = mock_wh
    monkeypatch.setattr(diagnostics, "WorkspaceClient", lambda **_: mock_ws)


def _all_functions_rows():
    return [{"function": f"test_cat.test_schema.{f}"} for f in queries.OMOP_FUNCTIONS]


def test_check_as_dict():
    c = diagnostics.Check(name="x", status="ok", detail="y")
    assert c.as_dict() == {"name": "x", "status": "ok", "detail": "y"}


def test_run_all_ok(fake_query, monkeypatch):
    fake_query.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        _all_functions_rows(),
        [{"condition_concept_id": 1}],
    ]
    _stub_warehouse(monkeypatch)

    checks = diagnostics.run("tok")
    assert len(checks) == 6
    assert all(c.status == "ok" for c in checks)
    detail_for = {c.name: c.detail for c in checks}
    assert "RUNNING" in detail_for["Warehouse reachable"]
    assert "round-trip" in detail_for["SELECT 1 round-trip"]
    assert f"all {len(queries.OMOP_FUNCTIONS)} functions visible" in detail_for["OMOP functions visible"]


def test_warehouse_failure_isolated_from_others(fake_query, monkeypatch):
    fake_query.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        _all_functions_rows(),
        [{"condition_concept_id": 1}],
    ]
    _stub_warehouse(monkeypatch, side_effect=PermissionError("no access"))

    checks = diagnostics.run("tok")
    wh = next(c for c in checks if c.name == "Warehouse reachable")
    assert wh.status == "fail"
    assert "PermissionError" in wh.detail
    others = [c for c in checks if c.name != "Warehouse reachable"]
    assert all(c.status == "ok" for c in others)


def test_missing_functions_reported_in_detail(fake_query, monkeypatch):
    fake_query.side_effect = [
        [{"ok": 1}],
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        [{"function": "test_cat.test_schema.omop_concept_search"}],  # only 1 of 6
        [{"condition_concept_id": 1}],
    ]
    _stub_warehouse(monkeypatch)

    checks = diagnostics.run("tok")
    fn = next(c for c in checks if c.name == "OMOP functions visible")
    assert fn.status == "ok"
    assert "missing" in fn.detail
    assert "omop_top_conditions" in fn.detail


def test_select_one_shape_mismatch(fake_query, monkeypatch):
    fake_query.side_effect = [
        [{"ok": 0}],  # wrong value
        [{"databaseName": "omop531"}],
        [{"tableName": "person"}],
        _all_functions_rows(),
        [{"condition_concept_id": 1}],
    ]
    _stub_warehouse(monkeypatch)

    checks = diagnostics.run("tok")
    s1 = next(c for c in checks if c.name == "SELECT 1 round-trip")
    assert s1.status == "fail"
    assert "RuntimeError" in s1.detail
