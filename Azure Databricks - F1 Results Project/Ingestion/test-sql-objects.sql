-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### create, show, describe commands

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS test1;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE test1;

-- COMMAND ----------

USE test1;
SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES IN test1;

-- COMMAND ----------

-- MAGIC %run "../Includes/configfile"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #race_results.write.format("parquet").saveAsTable("test1.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC race_results_python;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS SELECT * FROM race_results_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM race_results_sql;

-- COMMAND ----------

DROP TABLE IF EXISTS test1.race_results_sql;
SHOW TABLES IN test1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### external tables in sql:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").option("path", f"{presentation_folder}/race_results_external").saveAsTable("test1.race_results_ext")

-- COMMAND ----------

SELECT * FROM race_results_ext

-- COMMAND ----------


