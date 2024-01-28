# Databricks notebook source
1) set the spark config fs.azure.account.key inthe cluster
2) list files from demo container
3) read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricks1dl11.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


