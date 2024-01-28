# Databricks notebook source
# MAGIC %run "../Includes/configfile"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###ingest qualifying

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

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
])

qualifying = spark.read.schema(qualifying_schema).option("multiLine",True).json(f"{raw_folder}/{v_file_date}/qualifying")
display(qualifying)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### rename columns, add, save

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

qualifying_final = qualifying.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("constructorId","constructor_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#qualifying_final.write.parquet("/mnt/databricks1dl11/processed/qualifying", mode="overwrite")
#qualifying_final.write.format("parquet").saveAsTable("f1_processed.qualifying", mode="overwrite")
#display(spark.read.parquet("/mnt/databricks1dl11/processed/qualifying"))

# COMMAND ----------

output_data = partition_column_change(qualifying_final, "race_id")

# COMMAND ----------

#overwrite_partition(qualifying_final, "f1_processed", "qualifying2", "race_id")

# COMMAND ----------

merge_condition = "tab.qualify_id = res.qualify_id AND tab.race_id = res.race_id"
merge_delta_data(qualifying_final, "f1_processed", "qualifying2", processed_folder, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


