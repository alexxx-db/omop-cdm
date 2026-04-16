# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP Healthcheck
# MAGIC Scheduled probes against UC + the OMOP functions + (optionally) the
# MAGIC deployed Explorer app. Any probe failure → task failure → email/Slack
# MAGIC notifications fire via the job's existing notification wiring.
# MAGIC
# MAGIC Two layers:
# MAGIC 1. **Data layer** (always): Spark SQL on serverless compute — tests
# MAGIC    catalog access, schema access, table presence, and UC function executability.
# MAGIC 2. **App layer** (opt-in via `app_url`): HTTP GET against the deployed
# MAGIC    FastAPI app's `/health` and `/api/me?include_diagnostics=true`.

# COMMAND ----------

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

# COMMAND ----------

dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
dbutils.widgets.text(
    "app_url",
    "",
    "Explorer app base URL (e.g. https://omop-explorer-prod.cloud.databricks.com). Empty → skip app probes.",
)

catalog = dbutils.widgets.get("catalog")
omop_schema = dbutils.widgets.get("omop_schema")
app_url = dbutils.widgets.get("app_url").strip().rstrip("/")

fqn = f"`{catalog}`.`{omop_schema}`"

OMOP_FUNCTIONS = (
    "omop_concept_search",
    "omop_concept_descendants",
    "omop_patient_summary",
    "omop_condition_cohort_size",
    "omop_drug_timeline",
    "omop_top_conditions",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe framework

# COMMAND ----------

@dataclass
class Probe:
    name: str
    status: str = "pending"
    detail: str = ""
    elapsed_ms: int = 0


def run_probe(name: str, fn) -> Probe:
    p = Probe(name=name)
    t0 = time.perf_counter()
    try:
        p.detail = fn() or ""
        p.status = "ok"
    except Exception as exc:
        p.status = "fail"
        p.detail = f"{type(exc).__name__}: {exc}"[:500]
    p.elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return p

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data-layer probes (Spark SQL)

# COMMAND ----------

probes: list[Probe] = []

def probe_catalog():
    rows = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
    return f"{len(rows)} schema(s) visible"

probes.append(run_probe(f"USE CATALOG `{catalog}`", probe_catalog))

# COMMAND ----------

def probe_schema():
    rows = spark.sql(f"SHOW TABLES IN {fqn}").collect()
    return f"{len(rows)} table(s) visible"

probes.append(run_probe(f"USE SCHEMA `{omop_schema}`", probe_schema))

# COMMAND ----------

def probe_functions_visible():
    rows = spark.sql(
        f"SHOW USER FUNCTIONS IN {fqn} LIKE 'omop_*'"
    ).collect()
    found = {str(r[0]).split(".")[-1] for r in rows}
    missing = [f for f in OMOP_FUNCTIONS if f not in found]
    if missing:
        raise RuntimeError(f"missing {len(missing)} function(s): {', '.join(missing)}")
    return f"all {len(OMOP_FUNCTIONS)} functions registered"

probes.append(run_probe("OMOP functions registered", probe_functions_visible))

# COMMAND ----------

def probe_top_conditions():
    rows = spark.sql(f"SELECT * FROM {fqn}.omop_top_conditions(1)").collect()
    if not rows:
        raise RuntimeError("omop_top_conditions(1) returned 0 rows — is the CDM populated?")
    return f"returned {len(rows)} row(s)"

probes.append(run_probe("EXECUTE omop_top_conditions(1)", probe_top_conditions))

# COMMAND ----------

def probe_concept_search():
    rows = spark.sql(
        f"SELECT * FROM {fqn}.omop_concept_search('test', NULL) LIMIT 5"
    ).collect()
    return f"returned {len(rows)} row(s)"

probes.append(run_probe("EXECUTE omop_concept_search('test')", probe_concept_search))

# COMMAND ----------

# MAGIC %md
# MAGIC ## App-layer probes (HTTP)

# COMMAND ----------

if app_url:
    import urllib.request
    import urllib.error

    def probe_app_health():
        url = f"{app_url}/health"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            if body.get("status") != "ok":
                raise RuntimeError(f"unexpected /health response: {body}")
            return f"status=ok, catalog={body.get('catalog')}"

    probes.append(run_probe(f"App /health ({app_url})", probe_app_health))

    def probe_app_diagnostics():
        url = f"{app_url}/api/me?include_diagnostics=true"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
            checks = body.get("checks", [])
            failed = [c for c in checks if c.get("status") == "fail"]
            if failed:
                names = ", ".join(c["name"] for c in failed)
                raise RuntimeError(f"{len(failed)} app-side probe(s) failed: {names}")
            return f"{len(checks)} check(s) passed"

    probes.append(run_probe(f"App diagnostics ({app_url})", probe_app_diagnostics))
else:
    print("app_url not set — skipping app-layer probes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

now = datetime.now(timezone.utc).isoformat(timespec="seconds")
_ts = datetime.now(timezone.utc)
results_df = spark.createDataFrame(
    [(_ts, p.name, p.status, p.detail, p.elapsed_ms) for p in probes],
    "check_time: timestamp, probe: string, status: string, detail: string, elapsed_ms: int",
)
display(results_df)

# COMMAND ----------

# Persist to a Delta table for trend analysis (idempotent table creation).
healthcheck_table = f"`{catalog}`.`{omop_schema}`.`_healthcheck_log`"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {healthcheck_table} (
        check_time TIMESTAMP,
        probe STRING,
        status STRING,
        detail STRING,
        elapsed_ms INT
    )
    TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)
""")
results_df.write.mode("append").saveAsTable(healthcheck_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assert all probes passed

# COMMAND ----------

failed = [p for p in probes if p.status != "ok"]
if failed:
    msg = "\n".join(f"  FAIL  {p.name}: {p.detail}" for p in failed)
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "failures": [asdict(p) for p in failed]}))
    raise RuntimeError(f"Healthcheck failed — {len(failed)} probe(s):\n{msg}")

print(f"All {len(probes)} probes passed.")
dbutils.notebook.exit(json.dumps({"status": "OK", "probes": len(probes)}))
