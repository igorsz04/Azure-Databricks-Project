# Databricks notebook source
1) register azure ad application / service principal
2) generate secret password for application
3) set spark config with app/client id, directory / tenant id & secret
4) assign role 'storage blob data contributor' to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-id")
tenant_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricks1dl11.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricks1dl11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricks1dl11.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricks1dl11.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricks1dl11.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databricks1dl11.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


