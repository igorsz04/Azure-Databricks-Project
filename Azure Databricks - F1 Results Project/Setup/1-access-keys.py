# Databricks notebook source
1) set the spark config fs.azure.account.key
2) list files from demo container
3) read data from circuits.csv file

# COMMAND ----------

databricks_1_account_key = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks1dl11-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databricks1dl11.dfs.core.windows.net",
    databricks_1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricks1dl11.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


