# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
          (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA
          """)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_result_updated
as
SELECT races.race_year, constructors.name as team_name, drivers.driver_id, drivers.name as driver_name, races.raceID, results.position, results.points,
  10+1-results.position as calculated_points
FROM f1_processed.results JOIN f1_processed.drivers on results.driver_id = drivers.driver_id
JOIN f1_processed.constructors on results.constructor_id = constructors.constructor_id
JOIN f1_processed.races on results.race_id = races.raceID
WHERE results.position <= 10
AND results.file_date = '{v_file_date}'
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_result_updated 

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_presentation.calculated_race_results crr
# MAGIC USING race_result_updated rru
# MAGIC ON (crr.driver_id = rru.driver_id AND crr.race_id = rru.raceID)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET crr.position = rru.position,
# MAGIC              crr.points = rru.points,
# MAGIC              crr.calculated_points = rru.calculated_points,
# MAGIC              crr.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date) 
# MAGIC   VALUES (race_year, team_name, driver_id, driver_name, raceID, position, points, calculated_points, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results

# COMMAND ----------


