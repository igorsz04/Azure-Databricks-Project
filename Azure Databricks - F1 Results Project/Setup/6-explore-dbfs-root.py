# Databricks notebook source
1) list all folders in dbfs root
2) interact with dbfs file browser
3) upload file to dbfs root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/"))

# COMMAND ----------

display(spark.read.csvv("/FileStore/circuits.csv"))

# COMMAND ----------


