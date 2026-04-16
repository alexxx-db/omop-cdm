-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie curated views
-- MAGIC Flattens OMOP 5.3.1 tables into natural-language-friendly views for Genie Spaces.
-- MAGIC Every view lives under `{catalog}.{omop_schema}_genie` — a separate schema so Genie only sees the curated surface, not the raw CDM.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
-- MAGIC dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema (source)")
-- MAGIC dbutils.widgets.text("genie_schema_suffix", "_genie", "Suffix appended to omop_schema for the Genie schema")
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC omop_schema = dbutils.widgets.get("omop_schema")
-- MAGIC genie_schema = omop_schema + dbutils.widgets.get("genie_schema_suffix")
-- MAGIC
-- MAGIC src = f"`{catalog}`.`{omop_schema}`"
-- MAGIC dst = f"`{catalog}`.`{genie_schema}`"
-- MAGIC
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dst} COMMENT 'Genie-facing flattened views over the OMOP CDM.'")
-- MAGIC spark.sql(f"USE CATALOG `{catalog}`")
-- MAGIC spark.sql(f"USE SCHEMA `{genie_schema}`")
-- MAGIC print(f"Creating Genie views in {catalog}.{genie_schema} sourcing from {catalog}.{omop_schema}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Helper so every view carries a table-level COMMENT — Genie reads those
-- MAGIC # to decide which tables are relevant for a question.
-- MAGIC def create_view(name: str, comment: str, sql: str) -> None:
-- MAGIC     spark.sql(f"CREATE OR REPLACE VIEW {dst}.{name} COMMENT '{comment}' AS {sql}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. `patient_demographics`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_view(
-- MAGIC     "patient_demographics",
-- MAGIC     "One row per patient with gender, race, ethnicity, year of birth, and current age (years).",
-- MAGIC     f"""
-- MAGIC     SELECT
-- MAGIC       p.person_id,
-- MAGIC       cg.concept_name AS gender,
-- MAGIC       cr.concept_name AS race,
-- MAGIC       ce.concept_name AS ethnicity,
-- MAGIC       p.year_of_birth,
-- MAGIC       CAST(year(current_date()) - p.year_of_birth AS INT) AS age_years
-- MAGIC     FROM {src}.person p
-- MAGIC     LEFT JOIN {src}.concept cg ON cg.concept_id = p.gender_concept_id
-- MAGIC     LEFT JOIN {src}.concept cr ON cr.concept_id = p.race_concept_id
-- MAGIC     LEFT JOIN {src}.concept ce ON ce.concept_id = p.ethnicity_concept_id
-- MAGIC     """,
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. `conditions_readable`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_view(
-- MAGIC     "conditions_readable",
-- MAGIC     "One row per condition occurrence with the concept name resolved. Use this for questions about diagnoses.",
-- MAGIC     f"""
-- MAGIC     SELECT
-- MAGIC       co.condition_occurrence_id,
-- MAGIC       co.person_id,
-- MAGIC       co.condition_start_date,
-- MAGIC       co.condition_end_date,
-- MAGIC       co.condition_concept_id,
-- MAGIC       c.concept_name  AS condition_name,
-- MAGIC       c.vocabulary_id AS condition_vocabulary,
-- MAGIC       co.visit_occurrence_id
-- MAGIC     FROM {src}.condition_occurrence co
-- MAGIC     LEFT JOIN {src}.concept c ON c.concept_id = co.condition_concept_id
-- MAGIC     """,
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `drugs_readable`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_view(
-- MAGIC     "drugs_readable",
-- MAGIC     "One row per drug exposure with the drug's concept name, quantity, and days supply resolved.",
-- MAGIC     f"""
-- MAGIC     SELECT
-- MAGIC       de.drug_exposure_id,
-- MAGIC       de.person_id,
-- MAGIC       de.drug_exposure_start_date,
-- MAGIC       de.drug_exposure_end_date,
-- MAGIC       de.drug_concept_id,
-- MAGIC       c.concept_name AS drug_name,
-- MAGIC       de.quantity,
-- MAGIC       de.days_supply,
-- MAGIC       de.visit_occurrence_id
-- MAGIC     FROM {src}.drug_exposure de
-- MAGIC     LEFT JOIN {src}.concept c ON c.concept_id = de.drug_concept_id
-- MAGIC     """,
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. `visits_readable`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_view(
-- MAGIC     "visits_readable",
-- MAGIC     "One row per healthcare visit with visit type (inpatient/outpatient/ER) resolved. Use for questions about encounters, admissions, and utilization.",
-- MAGIC     f"""
-- MAGIC     SELECT
-- MAGIC       v.visit_occurrence_id,
-- MAGIC       v.person_id,
-- MAGIC       v.visit_start_date,
-- MAGIC       v.visit_end_date,
-- MAGIC       v.visit_concept_id,
-- MAGIC       c.concept_name AS visit_type,
-- MAGIC       DATEDIFF(v.visit_end_date, v.visit_start_date) AS length_of_stay_days
-- MAGIC     FROM {src}.visit_occurrence v
-- MAGIC     LEFT JOIN {src}.concept c ON c.concept_id = v.visit_concept_id
-- MAGIC     """,
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for p in ["account users"]:  # replace with the Genie-consuming group
-- MAGIC     spark.sql(f"GRANT USE SCHEMA ON SCHEMA {dst} TO `{p}`")
-- MAGIC     for v in ["patient_demographics", "conditions_readable", "drugs_readable", "visits_readable"]:
-- MAGIC         spark.sql(f"GRANT SELECT ON VIEW {dst}.{v} TO `{p}`")
