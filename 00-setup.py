# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Project Setup
# MAGIC Resolves Unity Catalog targets, ensures the bronze catalog/schema/volume exist, picks the Synthea source location for the requested `project` profile, and configures the MLflow experiment.
# MAGIC
# MAGIC Downstream notebooks read their own job parameters — this notebook no longer writes a `/tmp/*.json` handoff file (that pattern doesn't survive serverless task isolation).

# COMMAND ----------

dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
dbutils.widgets.text("results_schema", "omop531_results", "Results schema")
dbutils.widgets.text("landing_volume", "landing", "Landing volume (under bronze schema)")
dbutils.widgets.dropdown(
    "project",
    "omop-cdm-100K",
    ["omop-cdm-100K", "omop-cdm-10K", "psm"],
    "Dataset profile",
)
dbutils.widgets.text(
    "source_path",
    "",
    "Optional Synthea CSV source override (defaults to the landing volume)",
)

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
omop_schema = dbutils.widgets.get("omop_schema")
results_schema = dbutils.widgets.get("results_schema")
landing_volume = dbutils.widgets.get("landing_volume")
project = dbutils.widgets.get("project")
source_path_override = dbutils.widgets.get("source_path").strip()

volume_path = f"/Volumes/{catalog}/{bronze_schema}/{landing_volume}"
source_path = source_path_override or volume_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ensure Unity Catalog objects exist
# MAGIC The DAB declares these, but running the notebook interactively (outside a bundle deploy) still needs them. `IF NOT EXISTS` keeps this idempotent.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{omop_schema}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{results_schema}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`{landing_volume}`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source data
# MAGIC The original accelerator read Synthea CSVs from public S3 buckets. On serverless + Unity Catalog, arbitrary S3 access requires an [external location](https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations) and storage credential, so the canonical pattern is to stage the CSVs into a UC Volume once.
# MAGIC
# MAGIC | Profile | Original public S3 source | Records |
# MAGIC |---|---|---|
# MAGIC | `omop-cdm-100K` | `s3://hls-eng-data-public/data/rwe/all-states-90K/` | ~90K patients |
# MAGIC | `omop-cdm-10K` | `s3://hls-eng-data-public/data/synthea/` | ~10K patients |
# MAGIC | `psm` | `s3://hls-eng-data-public/data/rwe/dbx-covid-sim/` | COVID sim |
# MAGIC
# MAGIC To stage: `databricks fs cp -r s3://.../ dbfs:/Volumes/<catalog>/<bronze_schema>/<volume>/` or use the UC Volume browser.

# COMMAND ----------

PROJECT_SOURCE_HINTS = {
    "omop-cdm-100K": "s3://hls-eng-data-public/data/rwe/all-states-90K/",
    "omop-cdm-10K": "s3://hls-eng-data-public/data/synthea/",
    "psm": "s3://hls-eng-data-public/data/rwe/dbx-covid-sim/",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MLflow experiment
# MAGIC Using a Workspace path namespaced under the current user keeps experiments discoverable and avoids collisions across environments.

# COMMAND ----------

import mlflow

user = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .tags()
    .apply("user")
)
experiment_path = f"/Users/{user}/{project}"
experiment = mlflow.set_experiment(experiment_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

summary = {
    "catalog": catalog,
    "bronze_schema": bronze_schema,
    "omop_schema": omop_schema,
    "results_schema": results_schema,
    "landing_volume": volume_path,
    "project": project,
    "source_path": source_path,
    "source_hint_original_s3": PROJECT_SOURCE_HINTS.get(project, "n/a"),
    "mlflow_experiment": experiment.name,
    "mlflow_experiment_id": experiment.experiment_id,
}

displayHTML(
    "<h4>OMOP CDM project settings</h4><ul>"
    + "".join(f"<li><b>{k}</b> = <code>{v}</code></li>" for k, v in summary.items())
    + "</ul>"
)

dbutils.jobs.taskValues.set(key="source_path", value=source_path)
dbutils.jobs.taskValues.set(key="mlflow_experiment_id", value=experiment.experiment_id)

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
