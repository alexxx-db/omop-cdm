# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Synthea Records to Delta (Bronze)
# MAGIC Reads Synthea CSVs from a Unity Catalog Volume and writes each file as a managed Delta table in `{catalog}.{bronze_schema}`.
# MAGIC Serverless-compatible: no DBFS paths, no `inferSchema` (materializes once during read), no path-based writes.

# COMMAND ----------

dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("landing_volume", "landing", "Landing volume (under bronze schema)")
dbutils.widgets.text(
    "source_path",
    "",
    "Source path override (defaults to the landing volume)",
)
dbutils.widgets.text("project", "omop-cdm-100K", "Dataset profile (metadata only)")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
landing_volume = dbutils.widgets.get("landing_volume")
source_path_override = dbutils.widgets.get("source_path").strip()
project = dbutils.widgets.get("project")

try:
    upstream_source = dbutils.jobs.taskValues.get(
        taskKey="setup", key="source_path", default="", debugValue=""
    )
except Exception:
    upstream_source = ""

source_path = (
    source_path_override
    or upstream_source
    or f"/Volumes/{catalog}/{bronze_schema}/{landing_volume}"
)
print(f"Reading Synthea CSVs from: {source_path}")
print(f"Writing bronze tables into: {catalog}.{bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Discover source CSVs
# MAGIC `dbutils.fs.ls` against a UC Volume path is supported on serverless. Each entry becomes a managed Delta table named after the file stem.

# COMMAND ----------

import re

def _table_name(file_name: str) -> str:
    stem = file_name.rstrip("/").rsplit("/", 1)[-1]
    stem = re.sub(r"\.csv(\.gz)?$", "", stem, flags=re.IGNORECASE)
    sanitized = re.sub(r"[^0-9a-zA-Z_]+", "_", stem).strip("_").lower()
    return sanitized or "unnamed"

entries = dbutils.fs.ls(source_path)
csv_entries = [e for e in entries if e.name.lower().endswith((".csv", ".csv.gz"))]

if not csv_entries:
    raise RuntimeError(
        f"No CSV files found under {source_path}. "
        "Stage Synthea CSVs into the landing volume first — see 00-setup.py for details."
    )

datasets = [(e.path, _table_name(e.name)) for e in csv_entries]
display(spark.createDataFrame(datasets, ["source_path", "table_name"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write each CSV as a managed Delta table
# MAGIC - `inferSchema` + `samplingRatio=1.0` on first load → the subsequent CDM ETL (notebook 4) expects typed columns.
# MAGIC - `overwriteSchema` lets re-runs pick up upstream Synthea schema changes without dropping tables manually.
# MAGIC - Managed tables (no `LOCATION`) — UC picks the managed storage location for the `bronze` schema.

# COMMAND ----------

from pyspark.sql import DataFrame

def _ingest(path: str, table: str) -> int:
    df: DataFrame = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )
    fqn = f"`{catalog}`.`{bronze_schema}`.`{table}`"
    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqn)
    )
    return spark.table(fqn).count()

results = []
for path, table in datasets:
    print(f"ingesting {path} → {catalog}.{bronze_schema}.{table}")
    n = _ingest(path, table)
    results.append((table, n))

# COMMAND ----------

# DBTITLE 1,Row counts
display(
    spark.createDataFrame(results, ["table", "n_records"])
    .orderBy("n_records", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library License|Library License URL|Library Source URL|
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
# MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
# MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
