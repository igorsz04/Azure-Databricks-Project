# Databricks notebook source
1) set the spark config for SAS token
2) list files from demo container
3) read data from circuits.csv file

# COMMAND ----------

databricks1dl11_demo_sas_token = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricks1dl11.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricks1dl11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricks1dl11.dfs.core.windows.net", databricks1dl11_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricks1dl11.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


