"""Fixtures for FastAPI endpoint tests.

With the query logic extracted to `omop_lib`, these tests focus on the
HTTP contract: route wiring, input validation, OBO header handling, and
error translation. The `queries` / `diagnostics` modules are mocked
wholesale — their SQL is verified in `src/omop_lib/tests/`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABRICKS_CATALOG", "test_cat")
os.environ.setdefault("DATABRICKS_OMOP_SCHEMA", "test_schema")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "test_wh_id")
os.environ.setdefault("DATABRICKS_HOST", "https://test.databricks.com")

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


@pytest.fixture
def app_module(monkeypatch):
    import app as app_module

    monkeypatch.setattr(app_module, "_resolve_token", lambda _hdr: "fake-token")
    return app_module


@pytest.fixture
def fake_queries(app_module, monkeypatch):
    m = MagicMock()
    monkeypatch.setattr(app_module, "queries", m)
    return m


@pytest.fixture
def fake_diagnostics(app_module, monkeypatch):
    m = MagicMock()
    monkeypatch.setattr(app_module, "diagnostics", m)
    return m


@pytest.fixture
def client(app_module, fake_queries, fake_diagnostics):
    from fastapi.testclient import TestClient

    return TestClient(app_module.app)
