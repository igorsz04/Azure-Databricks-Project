# Databricks notebook source
# MAGIC %run "../Includes/configfile"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###ingest lap_times

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read  using spark df reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
])

lap_times = spark.read.schema(lap_times_schema).option("multiLine",True).csv(f"{raw_folder}/{v_file_date}/lap_times")
display(lap_times)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### rename columns, add, save

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_times_final = lap_times.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#lap_times_final.write.format("parquet").saveAsTable("f1_processed.lap_times", mode="overwrite")
#display(spark.read.parquet("/mnt/databricks1dl11/processed/lap_times"))

# COMMAND ----------

output_data = partition_column_change(lap_times_final, "race_id")

# COMMAND ----------

#overwrite_partition(lap_times_final, "f1_processed", "lap_times2", "race_id")

# COMMAND ----------

merge_condition = "tab.race_id = res.race_id AND tab.driver_id = res.driver_id AND tab.lap = res.lap"
merge_delta_data(lap_times_final, "f1_processed", "lap_times2", processed_folder, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
