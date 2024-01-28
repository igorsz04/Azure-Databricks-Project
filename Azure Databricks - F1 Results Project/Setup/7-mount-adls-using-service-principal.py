# Databricks notebook source
1) get client id, tenant id, client secret from key vault
2) set spark config with app/ client id, directory/ tenant id & secret
3) call file system utility mount to mount the storage
4) explore other file system utilities related to mount

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-id")
tenant_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@databricks1dl11.dfs.core.windows.net/",
  mount_point = "/mnt/databricks1dl11/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("/mnt/databricks1dl11/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/databricks1dl11/demo")

# COMMAND ----------


