# Databricks notebook source
# MAGIC %run "../Includes/configfile"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###ingest pit_stops.json

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read json using spark df reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("stop", StringType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("duration", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
])

pit_stops = spark.read.schema(pit_stops_schema).option("multiLine",True).json(f"{raw_folder}/{v_file_date}/pit_stops.json")
display(pit_stops)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### rename columns, add, save

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

pit_stops_final = pit_stops.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#pit_stops_final.write.format("parquet").saveAsTable("f1_processed.pit_stops", mode="overwrite")
#display(spark.read.parquet("/mnt/databricks1dl11/processed/pit_stops"))

# COMMAND ----------

#overwrite_partition(pit_stops_final, "f1_processed", "pit_stops2", "race_id")

# COMMAND ----------

merge_condition = "tab.race_id = res.race_id AND tab.driver_id = res.driver_id AND tab.stop = res.stop"
merge_delta_data(pit_stops_final, "f1_processed", "pit_stops2", processed_folder, merge_condition, "race_id")

# COMMAND ----------

pit_stops_final

# COMMAND ----------

dbutils.notebook.exit("Success")
