from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from omop_lib import config


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "test_cat")
    monkeypatch.setenv("DATABRICKS_OMOP_SCHEMA", "test_schema")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test_wh_id")
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    config.reset()
    yield
    config.reset()


@pytest.fixture
def fake_query(monkeypatch):
    """Replace `query` at every import site — queries.py and diagnostics.py
    both do `from .warehouse import query`, so patching the source module
    alone would miss them."""
    from omop_lib import diagnostics, queries, warehouse

    m = MagicMock(return_value=[])
    monkeypatch.setattr(warehouse, "query", m)
    monkeypatch.setattr(queries, "query", m)
    monkeypatch.setattr(diagnostics, "query", m)
    return m
