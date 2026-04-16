-- Databricks notebook source
-- MAGIC %md
-- MAGIC # OMOP UC Functions
-- MAGIC Registers a set of SQL UDFs under `{catalog}.{omop_schema}` that Genie Spaces, Agent Bricks, and Databricks Apps can all call.
-- MAGIC
-- MAGIC Each function carries `COMMENT` strings on the function itself and every parameter — Agent Bricks reads those to decide when to call the tool, so they matter.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
-- MAGIC dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC omop_schema = dbutils.widgets.get("omop_schema")
-- MAGIC
-- MAGIC fqn = f"`{catalog}`.`{omop_schema}`"
-- MAGIC print(f"Registering UC functions under {catalog}.{omop_schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. `omop_concept_search` — keyword lookup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_concept_search(
-- MAGIC   keyword STRING COMMENT 'Substring to match case-insensitively within concept_name. Required.',
-- MAGIC   vocabulary STRING DEFAULT NULL COMMENT 'Optional OMOP vocabulary_id filter, e.g. "SNOMED", "LOINC", "RxNorm". NULL matches any vocabulary.'
-- MAGIC )
-- MAGIC RETURNS TABLE (
-- MAGIC   concept_id LONG,
-- MAGIC   concept_name STRING,
-- MAGIC   vocabulary_id STRING,
-- MAGIC   domain_id STRING,
-- MAGIC   concept_class_id STRING,
-- MAGIC   standard_concept STRING
-- MAGIC )
-- MAGIC COMMENT 'Search standard OMOP concepts by keyword, optionally filtered by vocabulary. Returns up to 100 currently-valid matches, shortest concept names first (closest to the keyword).'
-- MAGIC RETURN
-- MAGIC   SELECT concept_id, concept_name, vocabulary_id, domain_id, concept_class_id, standard_concept
-- MAGIC   FROM {fqn}.concept
-- MAGIC   WHERE (vocabulary IS NULL OR vocabulary_id = vocabulary)
-- MAGIC     AND lower(concept_name) LIKE concat('%', lower(keyword), '%')
-- MAGIC     AND standard_concept = 'S'
-- MAGIC     AND current_date() BETWEEN valid_start_date AND valid_end_date
-- MAGIC   ORDER BY length(concept_name) ASC
-- MAGIC   LIMIT 100
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. `omop_concept_descendants` — ontology walk

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_concept_descendants(
-- MAGIC   ancestor_concept_id LONG COMMENT 'Concept ID whose descendants (and itself) should be returned.'
-- MAGIC )
-- MAGIC RETURNS TABLE (
-- MAGIC   descendant_concept_id LONG,
-- MAGIC   descendant_name STRING,
-- MAGIC   min_levels_of_separation INT,
-- MAGIC   max_levels_of_separation INT
-- MAGIC )
-- MAGIC COMMENT 'Return every OMOP concept that is a descendant (or self) of the given ancestor via concept_ancestor, with hop distance. Useful for expanding a parent clinical concept into all of its specific codes before building a cohort.'
-- MAGIC RETURN
-- MAGIC   SELECT ca.descendant_concept_id,
-- MAGIC          c.concept_name AS descendant_name,
-- MAGIC          ca.min_levels_of_separation,
-- MAGIC          ca.max_levels_of_separation
-- MAGIC   FROM {fqn}.concept_ancestor ca
-- MAGIC   JOIN {fqn}.concept c ON ca.descendant_concept_id = c.concept_id
-- MAGIC   WHERE ca.ancestor_concept_id = omop_concept_descendants.ancestor_concept_id
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. `omop_patient_summary` — demographics + activity counts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_patient_summary(
-- MAGIC   person_id LONG COMMENT 'OMOP person_id.'
-- MAGIC )
-- MAGIC RETURNS TABLE (
-- MAGIC   person_id LONG,
-- MAGIC   gender STRING,
-- MAGIC   race STRING,
-- MAGIC   ethnicity STRING,
-- MAGIC   year_of_birth INT,
-- MAGIC   age_years INT,
-- MAGIC   n_visits BIGINT,
-- MAGIC   n_conditions BIGINT,
-- MAGIC   n_drug_exposures BIGINT,
-- MAGIC   observation_start DATE,
-- MAGIC   observation_end DATE
-- MAGIC )
-- MAGIC COMMENT 'Return a single-row demographic + activity snapshot for a patient: gender, race, ethnicity, year of birth, computed age, and counts of visits, conditions, and drug exposures. Returns zero rows if person_id is unknown.'
-- MAGIC RETURN
-- MAGIC   SELECT
-- MAGIC     p.person_id,
-- MAGIC     cg.concept_name  AS gender,
-- MAGIC     cr.concept_name  AS race,
-- MAGIC     ce.concept_name  AS ethnicity,
-- MAGIC     p.year_of_birth,
-- MAGIC     CAST(year(current_date()) - p.year_of_birth AS INT) AS age_years,
-- MAGIC     (SELECT COUNT(*) FROM {fqn}.visit_occurrence         v WHERE v.person_id = p.person_id) AS n_visits,
-- MAGIC     (SELECT COUNT(*) FROM {fqn}.condition_occurrence     co WHERE co.person_id = p.person_id) AS n_conditions,
-- MAGIC     (SELECT COUNT(*) FROM {fqn}.drug_exposure            de WHERE de.person_id = p.person_id) AS n_drug_exposures,
-- MAGIC     (SELECT MIN(observation_period_start_date) FROM {fqn}.observation_period op WHERE op.person_id = p.person_id) AS observation_start,
-- MAGIC     (SELECT MAX(observation_period_end_date)   FROM {fqn}.observation_period op WHERE op.person_id = p.person_id) AS observation_end
-- MAGIC   FROM {fqn}.person p
-- MAGIC   LEFT JOIN {fqn}.concept cg ON cg.concept_id = p.gender_concept_id
-- MAGIC   LEFT JOIN {fqn}.concept cr ON cr.concept_id = p.race_concept_id
-- MAGIC   LEFT JOIN {fqn}.concept ce ON ce.concept_id = p.ethnicity_concept_id
-- MAGIC   WHERE p.person_id = omop_patient_summary.person_id
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. `omop_condition_cohort_size` — distinct-patient count
-- MAGIC Counts patients with the given condition concept OR any of its descendants.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_condition_cohort_size(
-- MAGIC   condition_concept_id LONG COMMENT 'Ancestor condition concept id. The count includes patients with any descendant condition concept.'
-- MAGIC )
-- MAGIC RETURNS BIGINT
-- MAGIC COMMENT 'Return the number of distinct patients with at least one condition_occurrence record whose concept_id is a descendant (or equal to) the given ancestor_concept_id. Uses the concept_ancestor hierarchy.'
-- MAGIC RETURN
-- MAGIC   SELECT COUNT(DISTINCT co.person_id)
-- MAGIC   FROM {fqn}.condition_occurrence co
-- MAGIC   WHERE co.condition_concept_id IN (
-- MAGIC     SELECT descendant_concept_id
-- MAGIC     FROM {fqn}.concept_ancestor
-- MAGIC     WHERE ancestor_concept_id = omop_condition_cohort_size.condition_concept_id
-- MAGIC   )
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. `omop_drug_timeline` — per-patient drug-era history

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_drug_timeline(
-- MAGIC   person_id LONG COMMENT 'OMOP person_id whose drug history is requested.',
-- MAGIC   drug_concept_id LONG DEFAULT NULL COMMENT 'Optional ancestor drug concept_id to filter by (includes descendants). NULL returns all drugs.'
-- MAGIC )
-- MAGIC RETURNS TABLE (
-- MAGIC   drug_era_start_date DATE,
-- MAGIC   drug_era_end_date   DATE,
-- MAGIC   drug_concept_id     LONG,
-- MAGIC   drug_name           STRING,
-- MAGIC   drug_exposure_count INT,
-- MAGIC   gap_days            INT
-- MAGIC )
-- MAGIC COMMENT 'Return the drug-era timeline for a patient, optionally restricted to an ancestor drug concept and its descendants. Ordered by start date ascending.'
-- MAGIC RETURN
-- MAGIC   SELECT
-- MAGIC     de.drug_era_start_date,
-- MAGIC     de.drug_era_end_date,
-- MAGIC     de.drug_concept_id,
-- MAGIC     c.concept_name AS drug_name,
-- MAGIC     de.drug_exposure_count,
-- MAGIC     de.gap_days
-- MAGIC   FROM {fqn}.drug_era de
-- MAGIC   JOIN {fqn}.concept c ON c.concept_id = de.drug_concept_id
-- MAGIC   WHERE de.person_id = omop_drug_timeline.person_id
-- MAGIC     AND (
-- MAGIC       omop_drug_timeline.drug_concept_id IS NULL
-- MAGIC       OR de.drug_concept_id IN (
-- MAGIC         SELECT descendant_concept_id
-- MAGIC         FROM {fqn}.concept_ancestor
-- MAGIC         WHERE ancestor_concept_id = omop_drug_timeline.drug_concept_id
-- MAGIC       )
-- MAGIC     )
-- MAGIC   ORDER BY de.drug_era_start_date ASC
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. `omop_top_conditions` — most prevalent conditions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE FUNCTION {fqn}.omop_top_conditions(
-- MAGIC   top_n INT DEFAULT 20 COMMENT 'Number of top conditions to return (by distinct patient count).'
-- MAGIC )
-- MAGIC RETURNS TABLE (
-- MAGIC   condition_concept_id LONG,
-- MAGIC   condition_name STRING,
-- MAGIC   n_patients BIGINT,
-- MAGIC   n_occurrences BIGINT
-- MAGIC )
-- MAGIC COMMENT 'Return the most prevalent standard condition concepts in the OMOP dataset, ranked by distinct patient count.'
-- MAGIC RETURN
-- MAGIC   SELECT
-- MAGIC     co.condition_concept_id,
-- MAGIC     c.concept_name AS condition_name,
-- MAGIC     COUNT(DISTINCT co.person_id) AS n_patients,
-- MAGIC     COUNT(*)                     AS n_occurrences
-- MAGIC   FROM {fqn}.condition_occurrence co
-- MAGIC   JOIN {fqn}.concept c ON c.concept_id = co.condition_concept_id
-- MAGIC   WHERE c.standard_concept = 'S'
-- MAGIC   GROUP BY co.condition_concept_id, c.concept_name
-- MAGIC   ORDER BY n_patients DESC
-- MAGIC   LIMIT omop_top_conditions.top_n
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Smoke test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM {fqn}.omop_concept_search('diabetes', 'SNOMED') LIMIT 5"))
-- MAGIC display(spark.sql(f"SELECT * FROM {fqn}.omop_top_conditions(10)"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Grants
-- MAGIC Grant `EXECUTE` on the function set (and `USE SCHEMA` on the OMOP schema) to whichever group will drive the Genie Space / Agent / App. Adjust the principal to your workspace.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC principals = ["account users"]  # replace with your group(s)
-- MAGIC functions = [
-- MAGIC     "omop_concept_search", "omop_concept_descendants", "omop_patient_summary",
-- MAGIC     "omop_condition_cohort_size", "omop_drug_timeline", "omop_top_conditions",
-- MAGIC ]
-- MAGIC for p in principals:
-- MAGIC     spark.sql(f"GRANT USE SCHEMA ON SCHEMA {fqn} TO `{p}`")
-- MAGIC     for fn in functions:
-- MAGIC         spark.sql(f"GRANT EXECUTE ON FUNCTION {fqn}.{fn} TO `{p}`")
