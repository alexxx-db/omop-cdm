"""Typed wrappers over the six `omop_*` UC functions.

Each helper returns plain `list[dict]` (or `dict`/`int`) so both the FastAPI
and Gradio apps can format as they see fit.
"""

from __future__ import annotations

from typing import Any

from .config import Settings, settings
from .exceptions import OmopNotFound
from .warehouse import query

OMOP_FUNCTIONS: tuple[str, ...] = (
    "omop_concept_search",
    "omop_concept_descendants",
    "omop_patient_summary",
    "omop_condition_cohort_size",
    "omop_drug_timeline",
    "omop_top_conditions",
)


def concept_search(
    keyword: str, vocab: str | None, token: str, s: Settings | None = None
) -> list[dict[str, Any]]:
    s = s or settings()
    return query(
        f"SELECT * FROM {s.fqn}.omop_concept_search(:keyword, :vocab)",
        {"keyword": keyword, "vocab": vocab or None},
        token,
        s,
    )


def concept_descendants(
    concept_id: int, token: str, s: Settings | None = None
) -> list[dict[str, Any]]:
    s = s or settings()
    return query(
        f"SELECT * FROM {s.fqn}.omop_concept_descendants(:cid) ORDER BY min_levels_of_separation",
        {"cid": concept_id},
        token,
        s,
    )


def patient_summary(
    person_id: int, token: str, s: Settings | None = None
) -> dict[str, Any]:
    s = s or settings()
    rows = query(
        f"SELECT * FROM {s.fqn}.omop_patient_summary(:pid)",
        {"pid": person_id},
        token,
        s,
    )
    if not rows:
        raise OmopNotFound(f"person_id {person_id} not found")
    return rows[0]


def drug_timeline(
    person_id: int,
    drug_concept_id: int | None,
    token: str,
    s: Settings | None = None,
) -> list[dict[str, Any]]:
    s = s or settings()
    return query(
        f"SELECT * FROM {s.fqn}.omop_drug_timeline(:pid, :did)",
        {"pid": person_id, "did": drug_concept_id},
        token,
        s,
    )


def condition_cohort_size(
    condition_concept_id: int, token: str, s: Settings | None = None
) -> int:
    s = s or settings()
    rows = query(
        f"SELECT {s.fqn}.omop_condition_cohort_size(:cid) AS n_patients",
        {"cid": condition_concept_id},
        token,
        s,
    )
    return int(rows[0]["n_patients"])


def top_conditions(
    top_n: int, token: str, s: Settings | None = None
) -> list[dict[str, Any]]:
    s = s or settings()
    return query(
        f"SELECT * FROM {s.fqn}.omop_top_conditions(:n)",
        {"n": top_n},
        token,
        s,
    )
