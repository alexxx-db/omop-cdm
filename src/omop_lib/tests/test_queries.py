from __future__ import annotations

import pytest

from omop_lib import queries
from omop_lib.exceptions import OmopNotFound


def test_concept_search_forwards_params(fake_query):
    fake_query.return_value = [{"concept_id": 1}]
    out = queries.concept_search("diabetes", "SNOMED", "tok")
    assert out == [{"concept_id": 1}]
    stmt, params, token, _settings = fake_query.call_args.args
    assert "omop_concept_search" in stmt
    assert params == {"keyword": "diabetes", "vocab": "SNOMED"}
    assert token == "tok"


def test_concept_search_blank_vocab_becomes_none(fake_query):
    queries.concept_search("diabetes", "", "tok")
    _, params, _, _ = fake_query.call_args.args
    assert params["vocab"] is None


def test_concept_descendants(fake_query):
    queries.concept_descendants(4229440, "tok")
    stmt, params, _, _ = fake_query.call_args.args
    assert "omop_concept_descendants" in stmt
    assert "ORDER BY min_levels_of_separation" in stmt
    assert params == {"cid": 4229440}


def test_patient_summary_happy(fake_query):
    fake_query.return_value = [{"person_id": 42, "gender": "F"}]
    assert queries.patient_summary(42, "tok") == {"person_id": 42, "gender": "F"}


def test_patient_summary_not_found_raises(fake_query):
    fake_query.return_value = []
    with pytest.raises(OmopNotFound, match="99999"):
        queries.patient_summary(99999, "tok")


def test_drug_timeline_with_filter(fake_query):
    queries.drug_timeline(42, 1124957, "tok")
    _, params, _, _ = fake_query.call_args.args
    assert params == {"pid": 42, "did": 1124957}


def test_drug_timeline_null_filter(fake_query):
    queries.drug_timeline(42, None, "tok")
    _, params, _, _ = fake_query.call_args.args
    assert params == {"pid": 42, "did": None}


def test_condition_cohort_size(fake_query):
    fake_query.return_value = [{"n_patients": 1234}]
    assert queries.condition_cohort_size(4229440, "tok") == 1234
    _, params, _, _ = fake_query.call_args.args
    assert params == {"cid": 4229440}


def test_top_conditions(fake_query):
    fake_query.return_value = [{"condition_concept_id": 1}]
    out = queries.top_conditions(5, "tok")
    assert len(out) == 1
    _, params, _, _ = fake_query.call_args.args
    assert params == {"n": 5}


def test_all_queries_use_fqn(fake_query):
    """Every helper should build SQL against the configured catalog/schema."""
    queries.concept_search("d", None, "t")
    queries.concept_descendants(1, "t")
    queries.drug_timeline(1, None, "t")
    queries.condition_cohort_size(1, "t")
    queries.top_conditions(1, "t")
    for call in fake_query.call_args_list:
        stmt = call.args[0]
        assert "`test_cat`.`test_schema`" in stmt
