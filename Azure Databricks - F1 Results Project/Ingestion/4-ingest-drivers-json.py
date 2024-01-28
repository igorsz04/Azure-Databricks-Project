# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configfile"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### read json file 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields = [StructField("driverId", StringType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True),
])

# COMMAND ----------

drivers = spark.read.json(f"{raw_folder}/{v_file_date}/drivers.json", schema = drivers_schema)

# COMMAND ----------

display(drivers)

# COMMAND ----------

drivers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### new columns and renaming

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp
drivers_2 = drivers.withColumnRenamed("driverID", "driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("ingestion_date", current_timestamp())
drivers_final = drivers_2.withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

display(drivers_final)

# COMMAND ----------

drivers_final = drivers_final.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  writing as parquet

# COMMAND ----------

drivers_final.write.format("delta").saveAsTable("f1_processed.drivers", mode="overwrite")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
