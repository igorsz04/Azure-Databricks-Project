# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS formula_demo
# MAGIC LOCATION "/mnt/databricks1dl11/demo"
# MAGIC

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/databricks1dl11/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("formula_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/databricks1dl11/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE formula_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/databricks1dl11/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_external;

# COMMAND ----------

results_external = spark.read.format("delta").load("/mnt/databricks1dl11/demo/results_external")

# COMMAND ----------

display(results_external)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("formula_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS formula_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE formula_demo.results_managed
# MAGIC   SET points = 11-position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula_demo.results_managed
# MAGIC WHERE points = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Upsert using merge

# COMMAND ----------

drivers_day1 = spark.read.option("inferSchema", True).json("/mnt/databricks1dl11/raw/2021-03-28/drivers.json") \
    .filter("driverId <= 10").select("driverId", "dob", "name.forename", "name.surname")
display(drivers_day1)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2 = spark.read.option("inferSchema", True).json("/mnt/databricks1dl11/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15").select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day2)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3 = spark.read.option("inferSchema", True).json("/mnt/databricks1dl11/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20").select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day3)

# COMMAND ----------

drivers_day1.createOrReplaceTempView("drivers_d1")
drivers_day2.createOrReplaceTempView("drivers_d2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO formula_demo.drivers_merge dm
# MAGIC USING drivers_d1 d1
# MAGIC ON dm.driverId = d1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET dm.dob = d1.dob,
# MAGIC              dm.forename = d1.forename,
# MAGIC              dm.surname = d1.surname,
# MAGIC              dm.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM formula_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO formula_demo.drivers_merge dm
# MAGIC USING drivers_d2 d2
# MAGIC ON dm.driverId = d2.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET dm.dob = d2.dob,
# MAGIC              dm.forename = d2.forename,
# MAGIC              dm.surname = d2.surname,
# MAGIC              dm.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM formula_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/databricks1dl11/demo/drivers_merge")

deltaTable.alias("ddm").merge(
    drivers_day3.alias("d3"),
    "ddm.driverId = d3.driverId") \
        .whenMatchedUpdate(set = {"dob" : "d3.dob", "forename" : "d3.forename", "surname" : "d3.surname", "updatedDate" : "current_timestamp()" }) \
        .whenNotMatchedInsert(values = {"driverId" : "d3.driverId", "dob":"d3.dob", "forename" : "d3.forename", "surname" : "d3.surname", "updatedDate" : "current_timestamp()"}
).execute()


# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM formula_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY formula_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula_demo.drivers_merge TIMESTAMP AS OF "2024-01-27T13:52:22.000+0000";

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2024-01-27T13:52:22.000+0000").load("/mnt/databricks1dl11/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --how to delete pernamently records (for example for GDPR etc)
# MAGIC VACUUM formula_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM formula_demo.drivers_merge RETAIN 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula_demo.drivers_merge WHERE driverId = 1;
# MAGIC SELECT * FROM formula_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO formula_demo.drivers_merge dm
# MAGIC USING formula_demo.drivers_merge VERSION AS OF 3 v3
# MAGIC   ON (dm.driverId = v3.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;
# MAGIC
# MAGIC SELECT * FROM formula_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO formula_demo.drivers_txn
# MAGIC SELECT * FROM formula_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert parquet to delta

# COMMAND ----------

# MAGIC %md
# MAGIC ###delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into formula_demo.drivers_convert_to_delta
# MAGIC select * from formula_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA formula_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("formula_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/databricks1dl11/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/databricks1dl11/demo/drivers_convert_to_delta_new`

# COMMAND ----------


