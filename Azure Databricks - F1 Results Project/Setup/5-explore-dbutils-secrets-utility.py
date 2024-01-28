# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "databricks-1-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks1dl11-account-key")

# COMMAND ----------


