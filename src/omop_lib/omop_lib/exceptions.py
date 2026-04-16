"""Exception hierarchy the apps translate into framework errors."""

from __future__ import annotations


class OmopError(Exception):
    """Base class for OMOP library errors."""


class OmopNotFound(OmopError):
    """Query returned no rows when one was expected."""


class OmopSqlError(OmopError):
    """SQL execution error from the warehouse (raised for e.g. missing grants)."""


class OmopConnectionError(OmopError):
    """Network / transport error reaching the warehouse."""
