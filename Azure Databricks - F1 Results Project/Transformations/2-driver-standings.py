# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/configfile"

# COMMAND ----------

race_results_df = spark.read.format("delta").load("/mnt/databricks1dl11/presentation/race_results")  \
.filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect() 

# COMMAND ----------

race_year_list = []
for race_year in race_results_df:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results = spark.read.format("delta").load(f"{presentation_folder}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings = race_results.groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("total_wins")) \
        .orderBy("total_points", ascending = False)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, asc

driver_ranking = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))
final_ranking = driver_standings.withColumn("rank", rank().over(driver_ranking))
display(final_ranking.filter("race_year = 2020"))

# COMMAND ----------


#overwrite_partition(final_ranking,"f1_presentation","driver_standings","race_year")

# COMMAND ----------

merge_condition = "tab.race_year = res.race_year AND tab.driver_name = res.driver_name"
merge_delta_data(final_ranking, "f1_presentation", "driver_standings", presentation_folder, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC f1_presentation.driver_standings

# COMMAND ----------


