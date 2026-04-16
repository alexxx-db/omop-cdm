"""Genie Space lifecycle for the OMOP curated views.

Content (title, description, instructions, sample questions, views) lives in
`genie_config.yml` alongside this script — edit there, not here.

Subcommands:
    validate  — check prerequisites (views exist, warehouse reachable) and exit
    upsert    — create-or-update the space by title (idempotent; default)
    list      — list all Genie Spaces in the workspace
    delete    — delete the space by title (or explicit --space-id)

Usage:
    python src/genie/create_genie_space.py upsert \\
        --profile DEFAULT --catalog hls_omop_prod \\
        --warehouse-id <sql-warehouse-id>

    python src/genie/create_genie_space.py validate \\
        --profile DEFAULT --catalog hls_omop_prod

    python src/genie/create_genie_space.py delete \\
        --profile DEFAULT --title "OMOP Clinical Genie"

Exit codes:
    0 — success
    1 — validation failure (missing views, warehouse unreachable, etc.)
    2 — CLI misuse
    3 — Genie API error (endpoint moved, permission denied, …)
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound

# ---------------------------------------------------------------------------
# REST endpoints — isolated so they're easy to patch if the API moves.
# ---------------------------------------------------------------------------
GENIE_LIST = "/api/2.0/genie/spaces"
GENIE_CREATE = "/api/2.0/genie/spaces"
GENIE_GET = "/api/2.0/genie/spaces/{space_id}"
GENIE_UPDATE = "/api/2.0/genie/spaces/{space_id}"
GENIE_DELETE = "/api/2.0/genie/spaces/{space_id}"

CONFIG_PATH = Path(__file__).parent / "genie_config.yml"


@dataclass
class Config:
    title: str
    description: str
    views: list[str]
    instructions: str
    sample_questions: list[str]
    grant_can_run_to: list[str]

    @classmethod
    def load(cls, path: Path) -> "Config":
        data = yaml.safe_load(path.read_text())
        required = {"title", "description", "views", "instructions", "sample_questions"}
        missing = required - data.keys()
        if missing:
            raise SystemExit(f"genie_config.yml is missing keys: {sorted(missing)}")
        return cls(
            title=data["title"],
            description=data["description"],
            views=list(data["views"]),
            instructions=data["instructions"],
            sample_questions=list(data["sample_questions"]),
            grant_can_run_to=list(data.get("grant_can_run_to") or []),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    print(f"[genie] {msg}", flush=True)


def _fqn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _table_identifiers(catalog: str, genie_schema: str, views: list[str]) -> list[str]:
    return [_fqn(catalog, genie_schema, v) for v in views]


def _find_space_by_title(w: WorkspaceClient, title: str) -> dict[str, Any] | None:
    """Idempotency anchor: match on exact title (case-sensitive). Genie allows
    duplicate titles, so if multiple match we pick the oldest and warn."""
    resp = w.api_client.do("GET", GENIE_LIST)
    spaces = resp.get("spaces", []) if isinstance(resp, dict) else []
    matches = [s for s in spaces if s.get("title") == title]
    if len(matches) > 1:
        _log(
            f"WARNING: {len(matches)} spaces share the title {title!r}; "
            f"updating the oldest (space_id={matches[0].get('space_id')})."
        )
    return matches[0] if matches else None


def _validate_views(
    w: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    views: list[str],
) -> list[str]:
    """Return list of missing view FQNs. Empty list == all present."""
    missing: list[str] = []
    for v in views:
        fqn = _fqn(catalog, schema, v)
        try:
            w.tables.get(full_name=fqn)
        except NotFound:
            missing.append(fqn)
    return missing


def _validate_warehouse(w: WorkspaceClient, warehouse_id: str) -> None:
    try:
        wh = w.warehouses.get(id=warehouse_id)
    except NotFound as exc:
        raise SystemExit(f"SQL warehouse {warehouse_id!r} not found") from exc
    if str(wh.state) not in {"RUNNING", "STARTING", "STOPPED"}:
        _log(f"NOTE: warehouse {warehouse_id} is in state {wh.state} — Genie may cold-start it.")


def _build_payload(cfg: Config, warehouse_id: str, identifiers: list[str]) -> dict[str, Any]:
    return {
        "title": cfg.title,
        "description": cfg.description,
        "warehouse_id": warehouse_id,
        "table_identifiers": identifiers,
        "instructions": cfg.instructions,
        "sample_questions": cfg.sample_questions,
    }


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------
def cmd_validate(args: argparse.Namespace, cfg: Config) -> int:
    w = WorkspaceClient(profile=args.profile)
    genie_schema = args.omop_schema + args.genie_schema_suffix

    _log(f"Checking views in {args.catalog}.{genie_schema} ...")
    missing = _validate_views(w, args.warehouse_id or "", args.catalog, genie_schema, cfg.views)
    if missing:
        _log("Missing views — run the `register_genie_views` job task first:")
        for m in missing:
            _log(f"  - {m}")
        return 1
    _log(f"All {len(cfg.views)} views present.")

    if args.warehouse_id:
        _log(f"Checking warehouse {args.warehouse_id} ...")
        _validate_warehouse(w, args.warehouse_id)
        _log("Warehouse OK.")
    else:
        _log("No --warehouse-id supplied; skipped warehouse check.")
    return 0


def cmd_list(args: argparse.Namespace, cfg: Config) -> int:
    w = WorkspaceClient(profile=args.profile)
    resp = w.api_client.do("GET", GENIE_LIST)
    spaces = resp.get("spaces", []) if isinstance(resp, dict) else []
    if not spaces:
        _log("No Genie Spaces in this workspace.")
        return 0
    _log(f"{len(spaces)} Genie Space(s):")
    for s in spaces:
        print(f"  {s.get('space_id')}\t{s.get('title')!r}")
    return 0


def cmd_upsert(args: argparse.Namespace, cfg: Config) -> int:
    w = WorkspaceClient(profile=args.profile)
    genie_schema = args.omop_schema + args.genie_schema_suffix
    identifiers = _table_identifiers(args.catalog, genie_schema, cfg.views)

    # Prerequisite checks — fail fast with clear messages.
    missing = _validate_views(w, args.warehouse_id, args.catalog, genie_schema, cfg.views)
    if missing:
        _log("ERROR: missing curated views; run `register_genie_views` first:")
        for m in missing:
            _log(f"  - {m}")
        return 1
    _validate_warehouse(w, args.warehouse_id)

    payload = _build_payload(cfg, args.warehouse_id, identifiers)
    existing = _find_space_by_title(w, cfg.title)

    if args.dry_run:
        action = "update" if existing else "create"
        _log(f"[dry-run] would {action} space {cfg.title!r} with payload:")
        print(json.dumps(payload, indent=2))
        return 0

    try:
        if existing:
            space_id = existing["space_id"]
            _log(f"Updating existing space {space_id} ({cfg.title!r}) ...")
            resp = w.api_client.do(
                "PATCH", GENIE_UPDATE.format(space_id=space_id), body=payload
            )
        else:
            _log(f"Creating new space {cfg.title!r} ...")
            resp = w.api_client.do("POST", GENIE_CREATE, body=payload)
    except DatabricksError as exc:
        _log(f"ERROR: Genie API call failed: {exc}")
        _log(
            "If the Genie space-management REST paths have changed, patch "
            "GENIE_CREATE / GENIE_UPDATE at the top of this script."
        )
        return 3

    space_id = resp.get("space_id") or (existing and existing["space_id"])
    _log(f"OK. space_id={space_id}")
    _log(f"Open: {w.config.host}/genie/rooms/{space_id}")

    if cfg.grant_can_run_to:
        _log(f"Applying CAN_RUN grants to {len(cfg.grant_can_run_to)} principal(s) ...")
        for principal in cfg.grant_can_run_to:
            try:
                w.api_client.do(
                    "PUT",
                    f"/api/2.0/permissions/genie/{space_id}",
                    body={"access_control_list": [
                        {"group_name" if "@" not in principal else "user_name": principal,
                         "permission_level": "CAN_RUN"}
                    ]},
                )
            except DatabricksError as exc:
                _log(f"  WARNING: grant to {principal!r} failed: {exc}")
    return 0


def cmd_delete(args: argparse.Namespace, cfg: Config) -> int:
    w = WorkspaceClient(profile=args.profile)
    space_id = args.space_id
    if not space_id:
        existing = _find_space_by_title(w, args.title or cfg.title)
        if not existing:
            _log(f"No space found with title {args.title or cfg.title!r}; nothing to delete.")
            return 0
        space_id = existing["space_id"]

    if args.dry_run:
        _log(f"[dry-run] would delete space_id={space_id}")
        return 0

    w.api_client.do("DELETE", GENIE_DELETE.format(space_id=space_id))
    _log(f"Deleted space_id={space_id}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="DEFAULT", help="~/.databrickscfg profile")
    p.add_argument("--config", type=Path, default=CONFIG_PATH, help="Path to genie_config.yml")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp_validate = sub.add_parser("validate", help="Check prerequisites, no changes.")
    sp_validate.add_argument("--catalog", required=True)
    sp_validate.add_argument("--omop-schema", default="omop531")
    sp_validate.add_argument("--genie-schema-suffix", default="_genie")
    sp_validate.add_argument("--warehouse-id", default=None)

    sp_upsert = sub.add_parser("upsert", help="Create or update the space by title.")
    sp_upsert.add_argument("--catalog", required=True)
    sp_upsert.add_argument("--omop-schema", default="omop531")
    sp_upsert.add_argument("--genie-schema-suffix", default="_genie")
    sp_upsert.add_argument("--warehouse-id", required=True)
    sp_upsert.add_argument("--dry-run", action="store_true")

    sub.add_parser("list", help="List all Genie Spaces.")

    sp_delete = sub.add_parser("delete", help="Delete a space (by title from config, or --space-id).")
    sp_delete.add_argument("--title", default=None, help="Override config title for the match.")
    sp_delete.add_argument("--space-id", default=None, help="Delete by explicit space_id.")
    sp_delete.add_argument("--dry-run", action="store_true")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = Config.load(args.config)

    dispatch = {
        "validate": cmd_validate,
        "upsert": cmd_upsert,
        "list": cmd_list,
        "delete": cmd_delete,
    }
    return dispatch[args.cmd](args, cfg)


if __name__ == "__main__":
    sys.exit(main())
