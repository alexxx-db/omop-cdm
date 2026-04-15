-- Databricks notebook source
-- MAGIC %md 
-- MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # OMOP Vocabulary Setup
-- MAGIC Construct vocabulary tables, based on tables downloaded from [Athena](https://athena.ohdsi.org/search-terms/start) website and available here on `s3://hls-eng-data-public/data/rwe/omop-vocabs/`
-- MAGIC If you like to download a different dataset, downoad the vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) and
-- MAGIC use [databricks dbfs api](https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfs-api) utilities to upload downloaded vocabularies to `dbfs` under your `vocab_path`.
-- MAGIC 
-- MAGIC <img align="right" width="700"  src="https://drive.google.com/uc?export=view&id=16TU2l7XHjQLugmS_McXegBXKMglD--Fr">

-- COMMAND ----------

-- DBTITLE 1, config
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "hls_omop_dev", "Unity Catalog")
-- MAGIC dbutils.widgets.text("omop_schema", "omop531", "OMOP CDM schema")
-- MAGIC dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema (holds the landing volume)")
-- MAGIC dbutils.widgets.text("landing_volume", "landing", "Landing volume")
-- MAGIC dbutils.widgets.text(
-- MAGIC     "vocab_path",
-- MAGIC     "",
-- MAGIC     "Vocabulary source override (defaults to <landing_volume>/vocab)",
-- MAGIC )
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC omop_schema = dbutils.widgets.get("omop_schema")
-- MAGIC bronze_schema = dbutils.widgets.get("bronze_schema")
-- MAGIC landing_volume = dbutils.widgets.get("landing_volume")
-- MAGIC vocab_path_override = dbutils.widgets.get("vocab_path").strip()
-- MAGIC
-- MAGIC vocab_path = (
-- MAGIC     vocab_path_override
-- MAGIC     or f"/Volumes/{catalog}/{bronze_schema}/{landing_volume}/vocab"
-- MAGIC )
-- MAGIC print(f"Loading OHDSI vocabularies into {catalog}.{omop_schema} from {vocab_path}")
-- MAGIC spark.sql(f"USE CATALOG `{catalog}`")
-- MAGIC spark.sql(f"USE SCHEMA `{omop_schema}`")
-- MAGIC
-- MAGIC display(dbutils.fs.ls(vocab_path))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loading vocabularies as delta tables
-- MAGIC Vocabulary CSVs are expected under the landing volume at `vocab/`. Download the desired release from [Athena](https://athena.ohdsi.org/search-terms/start) and upload via the UC Volume browser or `databricks fs cp`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
-- MAGIC spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
-- MAGIC
-- MAGIC vocab_tables = [
-- MAGIC     "CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP",
-- MAGIC     "RELATIONSHIP", "CONCEPT_SYNONYM", "DOMAIN", "CONCEPT_CLASS", "DRUG_STRENGTH",
-- MAGIC ]
-- MAGIC DATE_COL_TABLES = {"CONCEPT", "CONCEPT_RELATIONSHIP", "DRUG_STRENGTH"}
-- MAGIC
-- MAGIC for table in vocab_tables:
-- MAGIC     src = f"{vocab_path}/{table}.csv.gz"
-- MAGIC     df = (
-- MAGIC         spark.read
-- MAGIC         .option("header", "true")
-- MAGIC         .option("inferSchema", "true")
-- MAGIC         .option("dateFormat", "yyyy-MM-dd")
-- MAGIC         .csv(src)
-- MAGIC     )
-- MAGIC     if table in DATE_COL_TABLES:
-- MAGIC         df = (
-- MAGIC             df.withColumn("valid_start_date", to_date("valid_start_date", "yyyy-MM-dd"))
-- MAGIC               .withColumn("valid_end_date",   to_date("valid_end_date",   "yyyy-MM-dd"))
-- MAGIC         )
-- MAGIC     fqn = f"`{catalog}`.`{omop_schema}`.`{table}`"
-- MAGIC     (
-- MAGIC         df.write.format("delta")
-- MAGIC           .mode("overwrite")
-- MAGIC           .option("overwriteSchema", "true")
-- MAGIC           .saveAsTable(fqn)
-- MAGIC     )

-- COMMAND ----------

-- DBTITLE 1,display tables and counts of records
-- MAGIC %python
-- MAGIC counts_sql = " UNION ALL ".join(
-- MAGIC     f"SELECT '{t}' AS table_name, COUNT(*) AS recs FROM `{catalog}`.`{omop_schema}`.`{t}`"
-- MAGIC     for t in vocab_tables
-- MAGIC )
-- MAGIC display(spark.sql(counts_sql + " ORDER BY recs DESC"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create vocab map tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_standard_vocab_map

-- COMMAND ----------

DROP TABLE IF EXISTS source_to_standard_vocab_map;

CREATE TABLE source_to_standard_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.concept_name AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.INVALID_REASON AS SOURCE_INVALID_REASON,
    c1.concept_id AS TARGET_CONCEPT_ID,
    c1.concept_name AS TARGET_CONCEPT_NAME,
    c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
    c1.domain_id AS TARGET_DOMAIN_ID,
    c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c1.INVALID_REASON AS TARGET_INVALID_REASON,
    c1.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT C
    JOIN CONCEPT_RELATIONSHIP CR ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    AND CR.invalid_reason IS NULL
    AND lower(cr.relationship_id) = 'maps to'
    JOIN CONCEPT C1 ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    AND C1.INVALID_REASON IS NULL
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP
  ;
SELECT * FROM source_to_standard_vocab_map LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_source_vocab_map

-- COMMAND ----------

DROP TABLE IF EXISTS source_to_source_vocab_map
;
CREATE TABLE source_to_source_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.invalid_reason AS SOURCE_INVALID_REASON,
    c.concept_ID as TARGET_CONCEPT_ID,
    c.concept_name AS TARGET_CONCEPT_NAME,
    c.vocabulary_id AS TARGET_VOCABULARY_ID,
    c.domain_id AS TARGET_DOMAIN_ID,
    c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c.INVALID_REASON AS TARGET_INVALID_REASON,
    c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT c
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP
;
SELECT * FROM source_to_source_vocab_map LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
-- MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
-- MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
-- MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
-- MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|

-- COMMAND ----------


