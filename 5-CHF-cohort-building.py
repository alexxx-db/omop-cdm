# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Congestive Heart Failure cohort study
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/f/fb/Blausen_0463_HeartAttack.png" width=300>
# MAGIC
# MAGIC Build a cohort of patients recently diagnosed with Congestive Heart Failure and compute ER-admission incidence rates by age/gender.
# MAGIC Ported from `5-CHF-cohort-building.r` to Python so it can run on Databricks Serverless with Unity Catalog.
# MAGIC Based on [The Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/SqlAndR.html#designing-a-simple-study).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters
# MAGIC All schema references use the 3-level Unity Catalog namespace (`catalog.schema.table`). Values are supplied by the job via `base_parameters`; defaults match the DAB.

# COMMAND ----------

dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
dbutils.widgets.text("results_schema", "omop531_results", "Results schema")

catalog = dbutils.widgets.get("catalog")
omop_schema = dbutils.widgets.get("omop_schema")
results_schema = dbutils.widgets.get("results_schema")

omop = f"`{catalog}`.`{omop_schema}`"
results = f"`{catalog}`.`{results_schema}`"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{omop_schema}`")

TARGET_CONDITION_CONCEPT_ID = 4229440  # Chronic congestive heart failure (disorder)
TARGET_DRUG_CONCEPT_ID = 1719286       # 10 ML Furosemide 10 MG/ML Injection
OUTCOME_CONCEPT_ID = 9203              # Emergency Room Visit
PRIOR_OBSERVATION_DAYS = 1095          # 3 years
OUTCOME_WINDOW_DAYS = 7

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Target cohort
# MAGIC Patients newly diagnosed with CCHF who have at least 1,095 days of prior observation and no Furosemide exposure before their first diagnosis. Uses parameter markers so concept IDs never land in the SQL text — avoids any concat/injection risk and lets Spark reuse the plan.

# COMMAND ----------

target_cohort_sql = f"""
WITH target_condition_set AS (
  SELECT person_id, MIN(condition_start_date) AS condition_start_date
  FROM {omop}.condition_occurrence
  WHERE condition_concept_id IN (
    SELECT descendant_concept_id
    FROM {omop}.concept_ancestor
    WHERE ancestor_concept_id = :target_condition_concept_id
  )
  GROUP BY person_id
),
target_drug_exposure AS (
  SELECT person_id, MIN(drug_exposure_start_date) AS drug_exposure_start_date
  FROM {omop}.drug_exposure
  WHERE drug_concept_id IN (
    SELECT descendant_concept_id
    FROM {omop}.concept_ancestor
    WHERE ancestor_concept_id = :target_drug_concept_id
  )
  GROUP BY person_id
)
SELECT
  1                                                   AS cohort_definition_id,
  t.person_id                                         AS subject_id,
  t.condition_start_date                              AS cohort_start_date,
  op.observation_period_end_date                      AS cohort_end_date
FROM target_condition_set t
INNER JOIN {omop}.observation_period op
  ON t.person_id = op.person_id
 AND t.condition_start_date >= DATE_ADD(op.observation_period_start_date, :prior_days)
 AND t.condition_start_date <= op.observation_period_end_date
LEFT JOIN target_drug_exposure d
  ON t.person_id = d.person_id
 AND t.condition_start_date > d.drug_exposure_start_date
WHERE d.person_id IS NULL
"""

target_cohort_df = spark.sql(
    target_cohort_sql,
    args={
        "target_condition_concept_id": TARGET_CONDITION_CONCEPT_ID,
        "target_drug_concept_id": TARGET_DRUG_CONCEPT_ID,
        "prior_days": PRIOR_OBSERVATION_DAYS,
    },
)
target_cohort_df.createOrReplaceTempView("target_cohort")
display(target_cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Outcome cohort
# MAGIC All ER visits — the outcome used for the incidence-rate calculation.

# COMMAND ----------

outcome_cohort_df = spark.sql(
    f"""
    SELECT
      2                                 AS cohort_definition_id,
      person_id                         AS subject_id,
      visit_start_date                  AS cohort_start_date,
      visit_end_date                    AS cohort_end_date
    FROM {omop}.visit_occurrence
    WHERE visit_concept_id = :outcome_concept_id
    GROUP BY person_id, visit_start_date, visit_end_date
    """,
    args={"outcome_concept_id": OUTCOME_CONCEPT_ID},
)
outcome_cohort_df.createOrReplaceTempView("outcome_cohort")
display(outcome_cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Incidence rate
# MAGIC Stratified by gender and 10-year age bucket, over a 7-day risk window following the index date.

# COMMAND ----------

chf_cohort_df = target_cohort_df.unionByName(outcome_cohort_df)
chf_cohort_df.createOrReplaceTempView("chf_cohort")

# COMMAND ----------

incidence_rate_df = spark.sql(
    f"""
    WITH tar AS (
      SELECT
        c.concept_name                                            AS gender,
        FLOOR((YEAR(ch.cohort_start_date) - p.year_of_birth) / 10) * 10 AS age,
        ch.subject_id,
        ch.cohort_start_date,
        LEAST(DATE_ADD(ch.cohort_start_date, :window_days), op.observation_period_end_date) AS cohort_end_date
      FROM chf_cohort ch
      INNER JOIN {omop}.observation_period op
        ON ch.subject_id = op.person_id
       AND op.observation_period_start_date < ch.cohort_start_date
       AND op.observation_period_end_date   > ch.cohort_start_date
      INNER JOIN {omop}.person  p ON ch.subject_id = p.person_id
      INNER JOIN {omop}.concept c ON p.gender_concept_id = c.concept_id
      WHERE ch.cohort_definition_id = 1
    ),
    days AS (
      SELECT gender, age, SUM(DATEDIFF(cohort_end_date, cohort_start_date)) AS days
      FROM tar GROUP BY gender, age
    ),
    events AS (
      SELECT tar.gender, tar.age, COUNT(*) AS events
      FROM tar
      INNER JOIN chf_cohort chf
        ON tar.subject_id       = chf.subject_id
       AND tar.cohort_start_date <= chf.cohort_start_date
       AND tar.cohort_end_date   >= chf.cohort_start_date
      WHERE chf.cohort_definition_id = 2
      GROUP BY tar.gender, tar.age
    )
    SELECT
      d.gender,
      d.age,
      d.days,
      COALESCE(e.events, 0) AS events,
      CASE WHEN d.days > 0
           THEN 1000.0 * COALESCE(e.events, 0) / d.days / :window_days
           ELSE 0 END       AS incidence_rate_per_1000_person_days
    FROM days d
    LEFT JOIN events e USING (gender, age)
    """,
    args={"window_days": OUTCOME_WINDOW_DAYS},
)
incidence_rate_df.createOrReplaceTempView("incident_rate")
display(incidence_rate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persist cohort + cohort definition (idempotent)
# MAGIC `MERGE` keeps re-runs of the job safe — the original R notebook used `INSERT`, which duplicated rows on every retry. We write definition rows to `cohort_definition` and member rows to `cohort`.

# COMMAND ----------

target_cohort_description = (
    "patients newly diagnosed with chronic congestive heart failure (CCHF); "
    "condition_occurrence of CCHF or any descendants, indexed at first diagnosis; "
    f">= {PRIOR_OBSERVATION_DAYS} days of prior observation; "
    "no Furosemide exposure prior to first CCHF diagnosis. "
    f"target_condition_concept_id={TARGET_CONDITION_CONCEPT_ID}; "
    f"target_drug_concept_id={TARGET_DRUG_CONCEPT_ID}."
)
outcome_cohort_description = (
    f"Emergency Room visits (visit_concept_id={OUTCOME_CONCEPT_ID})."
)

spark.sql(
    f"""
    MERGE INTO {omop}.cohort_definition t
    USING (
      SELECT 1 AS cohort_definition_id,
             'CHF_cohort'          AS cohort_definition_name,
             :target_desc          AS cohort_definition_description,
             1                     AS definition_type_concept_id,
             :target_sql           AS cohort_definition_syntax,
             1                     AS subject_concept_id,
             current_date()        AS cohort_initiation_date
      UNION ALL
      SELECT 2, 'ER_visit_cohort', :outcome_desc, 1, :outcome_sql, 1, current_date()
    ) s
    ON t.cohort_definition_id = s.cohort_definition_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
    args={
        "target_desc": target_cohort_description,
        "target_sql": target_cohort_sql,
        "outcome_desc": outcome_cohort_description,
        "outcome_sql": "SEE notebook 5-CHF-cohort-building.py",
    },
)

# COMMAND ----------

# Replace members for these two cohort_definition_ids so re-runs stay idempotent.
spark.sql(
    f"""
    MERGE INTO {omop}.cohort t
    USING chf_cohort s
      ON t.cohort_definition_id = s.cohort_definition_id
     AND t.subject_id           = s.subject_id
     AND t.cohort_start_date    = s.cohort_start_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

display(spark.sql(f"SELECT cohort_definition_id, COUNT(*) AS n FROM {omop}.cohort GROUP BY cohort_definition_id"))

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
