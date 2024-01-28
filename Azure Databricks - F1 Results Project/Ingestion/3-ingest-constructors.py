# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors = spark.read.json(f"{raw_folder}/{v_file_date}/constructors.json", schema = constructors_schema)

# COMMAND ----------

display(constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC #### dropping columns and renaming

# COMMAND ----------

constructors_2 = constructors.drop(constructors["url"])


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructors_final = constructors_2.withColumnRenamed("constructorID", "constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
display(constructors_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ####  writing as parquet

# COMMAND ----------

constructors_final.write.format("delta").saveAsTable("f1_processed.constructors", mode="overwrite")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
