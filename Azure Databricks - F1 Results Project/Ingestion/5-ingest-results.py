# Databricks notebook source
# MAGIC %run "../Includes/configfile"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### read json file 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusID", StringType(), True),
])

# COMMAND ----------

results = spark.read.json(f"{raw_folder}/{v_file_date}/results.json", schema = results_schema)

# COMMAND ----------

display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### new columns and renaming

# COMMAND ----------

results_final = results.drop("statusID")

display(results_final)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_final = results_final.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#removing duplicated
results_final = results_final.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####  writing as parquet

# COMMAND ----------

#results_final.write.format("parquet").saveAsTable("f1_processed.results", mode="overwrite", partitionBy="race_id")
#display(spark.read.parquet("/mnt/databricks1dl11/processed/results"))

# COMMAND ----------

# Incremental method

# COMMAND ----------

#output_data = partition_column_change(results_final, "race_id")

# COMMAND ----------

#overwrite_partition(results_final, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tab.result_id = res.result_id AND tab.race_id = res.race_id"
merge_delta_data(results_final, "f1_processed", "results", processed_folder, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) from f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking duplicates as there are some due to Ergast API data
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(1) > 1
# MAGIC order by race_id, driver_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")
