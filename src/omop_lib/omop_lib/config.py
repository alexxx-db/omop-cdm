"""Env-driven settings singleton."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    catalog: str
    omop_schema: str
    warehouse_id: str
    host: str | None

    @property
    def fqn(self) -> str:
        return f"`{self.catalog}`.`{self.omop_schema}`"


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings(
        catalog=os.environ["DATABRICKS_CATALOG"],
        omop_schema=os.environ["DATABRICKS_OMOP_SCHEMA"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        host=os.environ.get("DATABRICKS_HOST"),
    )


def reset() -> None:
    """Clear the cached settings — call from test fixtures after mutating env."""
    settings.cache_clear()
