# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("v_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configfile"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### read csv file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

races = spark.read.csv(f"{raw_folder}/{v_file_date}/races.csv", header=True, schema = races_schema)

# COMMAND ----------

display(races)

# COMMAND ----------

# MAGIC %md
# MAGIC #### adding dates and timestamp and selecting columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_time = races.withColumn("ingestion_date", current_timestamp()).withColumn("race_time", to_timestamp(concat(col("date"), lit(" "), col("time")),"yyyy-MM-dd HH:mm:ss")).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(races_time)

# COMMAND ----------

races_time_2 = races_time.select(races_time["raceID"],
                                 races_time["year"].alias("race_year"),
                                 races_time["round"],
                                 races_time["circuitID"],
                                 races_time["name"],
                                 races_time["ingestion_date"],
                                 races_time["race_time"],
                                 )

# COMMAND ----------

display(races_time_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####  writing as parquet

# COMMAND ----------


races_time_2.write.format("delta").saveAsTable("f1_processed.races", mode="overwrite", partitionBy="race_year")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
