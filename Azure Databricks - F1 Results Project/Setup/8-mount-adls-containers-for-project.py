# Databricks notebook source
1) 

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #getting secrets from key vault
    client_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-id")
    tenant_id = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope = "databricks-1-scope", key = "databricks-1-app-client-secret")

    #setting spark configg
    configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
     # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #mounting storage acc container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("databricks1dl11","raw")

# COMMAND ----------

mount_adls("databricks1dl11","processed")

# COMMAND ----------

mount_adls("databricks1dl11","presentation")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricks1dl11.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("/mnt/databricks1dl11/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/mnt/databricks1dl11/demo")

# COMMAND ----------


