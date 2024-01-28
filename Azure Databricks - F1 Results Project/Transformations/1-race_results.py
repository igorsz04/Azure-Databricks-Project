# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configfile"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

drivers = spark.read.format("delta").load(f"{processed_folder}/drivers") \
    .withColumnRenamed("number","drivers_number") \
    .withColumnRenamed("name","drivers_name") \
    .withColumnRenamed("nationality","drivers_nationality")


constructors = spark.read.format("delta").load(f"{processed_folder}/constructors") \
    .withColumnRenamed("name","team")


circuits = spark.read.format("delta").load(f"{processed_folder}/circuits") \
    .withColumnRenamed("location","circuits_location")


races = spark.read.format("delta").load(f"{processed_folder}/races") \
    .withColumnRenamed("name","race_name") \
        .withColumnRenamed("race_timestamp","race_date") \
            .withColumnRenamed("race_time","races_time")


results = spark.read.format("delta").load(f"{processed_folder}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time","race_time").withColumnRenamed("race_id","result_race_id").withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join tables: circuits to races

# COMMAND ----------

race_circuits = races.join(circuits, races.circuitID == circuits.circuit_id, "inner") \
    .select(races.raceID, races.race_year, races.race_name, races.races_time, circuits.circuits_location)

# COMMAND ----------

display(results)

# COMMAND ----------

race_results = results.join(race_circuits, results.result_race_id == race_circuits.raceID) \
    .join(drivers, results.driver_id == drivers.driver_id) \
    .join(constructors, results.constructor_id == constructors.constructor_id)

# COMMAND ----------

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_data = race_results.select("result_race_id", "race_year", "race_name", "races_time", "circuits_location", "drivers_name", "drivers_number", "drivers_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date").withColumn("created_date", current_timestamp()).withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

final_data_2 = final_data.withColumnRenamed("races_time","race_date") \
    .withColumnRenamed("circuits_location","circuit_location") \
        .withColumnRenamed("drivers_name","driver_name") \
        .withColumnRenamed("drivers_number","driver_number") \
        .withColumnRenamed("drivers_nationality","driver_nationality")


# COMMAND ----------

#old data save used in data lake

#overwrite_partition(final_data_2,"f1_presentation","race_results","result_race_id")

# COMMAND ----------

from pyspark.sql.functions import sum
final_data_2.filter("race_year == 2020 and driver_name == 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

merge_condition = "tab.result_race_id = res.result_race_id AND tab.driver_name = res.driver_name"
merge_delta_data(final_data_2, "f1_presentation", "race_results", presentation_folder, merge_condition, "result_race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select result_race_id, count(1)
# MAGIC from f1_presentation.race_results
# MAGIC group by result_race_id
# MAGIC order by result_race_id desc

# COMMAND ----------


