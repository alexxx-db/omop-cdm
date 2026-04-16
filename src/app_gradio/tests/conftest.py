"""Fixtures for the Gradio flavor's handler tests.

The app module runs real code at import time (builds the `gr.Blocks` UI and
instantiates `databricks.sdk.Config`), so we seed auth env vars *before* the
import, and patch `_token` / `_query` so handlers never hit the wire.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Config() auto-detects auth on construction; give it enough to succeed.
os.environ.setdefault("DATABRICKS_CATALOG", "test_cat")
os.environ.setdefault("DATABRICKS_OMOP_SCHEMA", "test_schema")
os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", "test_wh_id")
os.environ.setdefault("DATABRICKS_HOST", "https://test.databricks.com")
os.environ.setdefault("DATABRICKS_TOKEN", "fake-pat")

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


@pytest.fixture
def app_module(monkeypatch):
    import app as app_module

    monkeypatch.setattr(app_module, "_token", lambda _req: "fake-token")
    return app_module


@pytest.fixture
def fake_query(app_module, monkeypatch):
    """Replaces app._query. Per test set .return_value or .side_effect."""
    import pandas as pd

    m = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr(app_module, "_query", m)
    return m


@pytest.fixture
def fake_request():
    """Minimal `gr.Request` stand-in. Tests that care about identity mutate
    `req.headers` directly."""
    req = MagicMock()
    req.headers = {}
    return req
