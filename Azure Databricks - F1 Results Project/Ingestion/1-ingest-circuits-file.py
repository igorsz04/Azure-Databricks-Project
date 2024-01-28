# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### read csv file using spark dataframe reader

# COMMAND ----------

# MAGIC %run "../Includes/configfile"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits = spark.read.csv(f"{raw_folder}/{v_file_date}/circuits.csv", header=True, schema = circuits_schema)

# COMMAND ----------

circuits.printSchema()

# COMMAND ----------

display(circuits)

# COMMAND ----------

circuits.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only specific columns I want and renaming columns

# COMMAND ----------

circuits_select = circuits.select("circuitID", "circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_select_2 = circuits.select(circuits["circuitID"],
                                    circuits["circuitRef"].alias("circuit_ref"),
                                    circuits["name"],
                                    circuits["location"],
                                    circuits["country"].alias("race_country"),
                                    circuits["lat"].alias("latitude"),
                                    circuits["lng"].alias("longitude"),
                                    circuits["alt"])

# COMMAND ----------

circuits_select_2.show()

# COMMAND ----------

circuits_select_2 = circuits_select_2.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #### adding ingestion date to DF and writing as parquet

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuits_final = add_ingestion_date(circuits_select_2)
circuits_final = circuits_final.withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
display(circuits_final)

# COMMAND ----------

circuits_final.write.format("delta").saveAsTable("f1_processed.circuits", mode="overwrite")

# COMMAND ----------

# %fs
# ls mnt/databricks1dl11/processed/circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


