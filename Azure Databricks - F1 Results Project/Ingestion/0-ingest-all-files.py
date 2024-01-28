# Databricks notebook source
dbutils.notebook.run("1-ingest-circuits-file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("2-ingest-races", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("3-ingest-constructors", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("4-ingest-drivers-json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("5-ingest-results", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("6-ingest-pitstops", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("7-ingest-laptimes", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("8-ingest-qualifying", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.pit_stops2
# MAGIC --group by race_id
# MAGIC --order by race_id desc

# COMMAND ----------


