# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Drug Usage Trends
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/7/76/Medicine_Drugs.svg" width=300>
# MAGIC
# MAGIC Trends in drug usage across population segments, drug co-prescription patterns, and frequent itemset mining over drug eras.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
dbutils.widgets.text("results_schema", "omop531_results", "Results schema")

catalog = dbutils.widgets.get("catalog")
omop_schema = dbutils.widgets.get("omop_schema")
results_schema = dbutils.widgets.get("results_schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{omop_schema}`")

OXYCODONE_CONCEPT_ID = 1124957
HYDROCODONE_CONCEPT_ID = 1174888
COPRESCRIPTION_WINDOW_DAYS = 5
FPGROWTH_MIN_SUPPORT = 0.02
FPGROWTH_MIN_CONFIDENCE = 0.20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drug usage by age band and gender
# MAGIC Usage of Oxycodone over time, stratified by 10-year age band and gender. Uses a Spark SQL parameter marker for the concept id so the filter is bound safely.

# COMMAND ----------

oxycodone_usage_df = spark.sql(
    """
    SELECT
      tt.drug_concept_name,
      tt.drug_concept_id,
      COUNT(1)    AS s_count,
      tt.age_band,
      tt.year_of_era,
      tt.gender
    FROM (
      SELECT
        FLOOR((YEAR(t.drug_era_start_date) - p.year_of_birth) / 10)   AS age_band,
        YEAR(t.drug_era_start_date)                                   AS year_of_era,
        p.gender_concept_id,
        t.drug_concept_id,
        c.concept_name  AS drug_concept_name,
        c1.concept_name AS gender
      FROM drug_era t
      JOIN person  p  ON t.person_id        = p.person_id
      JOIN concept c  ON c.concept_id       = t.drug_concept_id
      JOIN concept c1 ON c1.concept_id      = p.gender_concept_id
      WHERE c.concept_id = :drug_concept_id
    ) tt
    WHERE tt.year_of_era > 2000
    GROUP BY tt.age_band, tt.year_of_era, tt.gender, tt.drug_concept_name, tt.drug_concept_id
    ORDER BY tt.age_band, tt.year_of_era, tt.drug_concept_id
    """,
    args={"drug_concept_id": OXYCODONE_CONCEPT_ID},
)
display(oxycodone_usage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top drugs prescribed in the past 5 years

# COMMAND ----------

top_recent_drugs_df = spark.sql(
    """
    WITH tt AS (
      SELECT
        YEAR(t.drug_era_start_date) AS year_of_era,
        t.drug_concept_id,
        c.concept_name AS drug_concept_name
      FROM drug_era t
      JOIN concept c ON t.drug_concept_id = c.concept_id
    )
    SELECT
      tt.drug_concept_name,
      tt.drug_concept_id,
      tt.year_of_era,
      COUNT(1) AS drug_count
    FROM tt
    WHERE YEAR(current_date()) - tt.year_of_era <= 5
    GROUP BY tt.drug_concept_name, tt.year_of_era, tt.drug_concept_id
    ORDER BY tt.year_of_era, drug_count DESC
    """
)
display(top_recent_drugs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Age distribution — Hydrocodone vs. Oxycodone

# COMMAND ----------

import plotly.express as px

age_distribution_df = spark.sql(
    """
    SELECT
      YEAR(MIN(t.drug_era_start_date)
           OVER (PARTITION BY t.person_id, t.drug_concept_id)) - p.year_of_birth AS age,
      c.concept_name AS d_concept_name
    FROM drug_era t
    JOIN person  p ON t.person_id      = p.person_id
    JOIN concept c ON t.drug_concept_id = c.concept_id
    WHERE t.drug_concept_id IN (:d1, :d2)
    """,
    args={"d1": OXYCODONE_CONCEPT_ID, "d2": HYDROCODONE_CONCEPT_ID},
)

age_hist_pdf = (
    age_distribution_df.groupBy("age", "d_concept_name").count().toPandas()
)
fig = px.histogram(
    age_hist_pdf,
    x="age",
    y="count",
    color="d_concept_name",
    marginal="box",
    hover_data=age_hist_pdf.columns,
)
fig.update_layout(
    autosize=False,
    width=700,
    height=700,
    margin=dict(l=50, r=50, b=100, t=100, pad=4),
    paper_bgcolor="LightSteelBlue",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Co-prescription analysis
# MAGIC
# MAGIC ### 4.1 Pairwise association via cosine support
# MAGIC Two drug eras are considered co-prescribed when their start dates **and** end dates are within `COPRESCRIPTION_WINDOW_DAYS`. Builds a temp view so downstream cells can re-use it.

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMPORARY VIEW coprescribed AS
    WITH drugs_and_names AS (
      SELECT person_id,
             c.concept_name AS drug_concept_name,
             drug_concept_id,
             drug_era_start_date,
             drug_era_end_date
      FROM drug_era
      JOIN concept c ON drug_concept_id = c.concept_id
    ),
    drugs AS (
      SELECT d1.person_id,
             d1.drug_concept_id      AS drug_id1,
             d2.drug_concept_id      AS drug_id2,
             d1.drug_concept_name    AS drug1,
             d2.drug_concept_name    AS drug2
      FROM drugs_and_names d1
      JOIN drugs_and_names d2
        ON d1.person_id = d2.person_id
       AND ABS(DATEDIFF(d1.drug_era_start_date, d2.drug_era_start_date)) <= :window_days
       AND ABS(DATEDIFF(d1.drug_era_end_date,   d2.drug_era_end_date))   <= :window_days
    ),
    counts AS (
      SELECT drug1, drug2, COUNT(*) AS count_d1d2
      FROM drugs
      GROUP BY drug1, drug2
    ),
    pairwise_sums AS (
      SELECT drug1, drug2, count_d1d2,
             SUM(count_d1d2) OVER (PARTITION BY drug1) AS sum_d1,
             SUM(count_d1d2) OVER (PARTITION BY drug2) AS sum_d2,
             SUM(count_d1d2) OVER ()                   AS sum_all
      FROM counts
    )
    SELECT drug1, drug2, count_d1d2,
           count_d1d2 / sum_d1 AS supp_d1d2,
           count_d1d2 / sum_d2 AS supp_d2d1,
           sum_d1     / sum_all AS supp_d1,
           sum_d2     / sum_all AS supp_d2
    FROM pairwise_sums
    """,
    args={"window_days": COPRESCRIPTION_WINDOW_DAYS},
)

display(spark.table("coprescribed"))

# COMMAND ----------

import pandas as pd

cosine_df = spark.sql(
    """
    SELECT drug1,
           drug2,
           supp_d1d2,
           LOG(supp_d1d2 / SQRT(supp_d1 * supp_d2)) AS lod_cosine_d1d2,
           LOG(supp_d1d2) - LOG(supp_d1)            AS lod_d1d2
    FROM coprescribed
    ORDER BY lod_cosine_d1d2 DESC
    """
).toPandas()

cosine_matrix = pd.crosstab(
    cosine_df.drug1,
    cosine_df.drug2,
    values=cosine_df.lod_cosine_d1d2,
    aggfunc="sum",
)
fig = px.imshow(cosine_matrix)
fig.update_layout(
    autosize=False,
    width=700,
    height=700,
    margin=dict(l=50, r=50, b=100, t=100, pad=4),
    paper_bgcolor="LightSteelBlue",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Frequent pattern mining with FP-Growth
# MAGIC Uses the DataFrame-based `pyspark.ml.fpm.FPGrowth` — the legacy RDD-based `pyspark.mllib.fpm.FPGrowth` is not supported on Serverless.
# MAGIC
# MAGIC Baskets are built by bucketing each drug era's start/end into a `COPRESCRIPTION_WINDOW_DAYS` window (pure Spark SQL — no Python UDF in the hot path), then collecting the distinct drug names per `(person, start_bucket, end_bucket)`.

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import array_distinct, col, collect_set, floor, unix_timestamp

bucket_seconds = COPRESCRIPTION_WINDOW_DAYS * 24 * 60 * 60

drugs_and_names = spark.sql(
    """
    SELECT person_id,
           c.concept_name AS drug_concept_name,
           drug_concept_id,
           drug_era_start_date,
           drug_era_end_date
    FROM drug_era
    JOIN concept c ON drug_concept_id = c.concept_id
    """
)

drug_baskets = (
    drugs_and_names
    .withColumn(
        "start_key",
        floor(unix_timestamp("drug_era_start_date") / bucket_seconds),
    )
    .withColumn(
        "end_key",
        floor(unix_timestamp("drug_era_end_date") / bucket_seconds),
    )
    .groupBy("person_id", "start_key", "end_key")
    .agg(array_distinct(collect_set("drug_concept_name")).alias("items"))
    .filter("size(items) > 1")
)

fp = FPGrowth(
    itemsCol="items",
    minSupport=FPGROWTH_MIN_SUPPORT,
    minConfidence=FPGROWTH_MIN_CONFIDENCE,
)
model = fp.fit(drug_baskets)

# COMMAND ----------

display(model.freqItemsets.orderBy(col("freq").desc()))

# COMMAND ----------

display(model.associationRules.orderBy(col("lift").desc()))

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
