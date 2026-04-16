from __future__ import annotations

import pytest

from omop_lib import config


def test_settings_reads_env():
    s = config.settings()
    assert s.catalog == "test_cat"
    assert s.omop_schema == "test_schema"
    assert s.warehouse_id == "test_wh_id"
    assert s.host == "https://test.databricks.com"


def test_fqn_uses_backticks():
    s = config.settings()
    assert s.fqn == "`test_cat`.`test_schema`"


def test_missing_required_var_raises(monkeypatch):
    monkeypatch.delenv("DATABRICKS_CATALOG", raising=False)
    config.reset()
    with pytest.raises(KeyError, match="DATABRICKS_CATALOG"):
        config.settings()


def test_settings_is_cached():
    a = config.settings()
    b = config.settings()
    assert a is b
