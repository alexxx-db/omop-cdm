"""Shared fixtures for the OMOP Explorer smoke tests.

The app reads its config from env vars at import time, so we set those before
letting pytest import the module. Auth and SQL execution are both mocked —
these are contract tests, not integration tests.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Env must be set BEFORE the app module is imported.
os.environ.setdefault("DATABRICKS_CATALOG", "test_cat")
os.environ.setdefault("DATABRICKS_OMOP_SCHEMA", "test_schema")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "test_wh_id")

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


@pytest.fixture
def app_module(monkeypatch):
    """Import the app module with OBO token resolution bypassed."""
    import app as app_module

    monkeypatch.setattr(app_module, "_resolve_token", lambda _hdr: "fake-token")
    return app_module


@pytest.fixture
def fake_run(app_module, monkeypatch):
    """Replaces app._run with a MagicMock. Set `.return_value` or `.side_effect`
    per test to control what the 'warehouse' returns."""
    m = MagicMock(return_value=[])
    monkeypatch.setattr(app_module, "_run", m)
    return m


@pytest.fixture
def client(app_module, fake_run):
    from fastapi.testclient import TestClient

    return TestClient(app_module.app)
