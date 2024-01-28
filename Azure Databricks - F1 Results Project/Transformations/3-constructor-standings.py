# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/configfile"

# COMMAND ----------

race_results = spark.read.format("delta").load(f"{presentation_folder}/race_results") \
.filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect() 

# COMMAND ----------

display(race_results)

# COMMAND ----------

race_year_list = []
for race_year in race_results:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results = spark.read.format("delta").load(f"{presentation_folder}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings = race_results.groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("total_wins")) \
        .orderBy("total_points", ascending = False)
display(constructor_standings.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, asc

team_ranking = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))
final_ranking = constructor_standings.withColumn("rank", rank().over(team_ranking))
display(final_ranking.filter("race_year = 2020"))

# COMMAND ----------

#legacy of data lake commands
#overwrite_partition(final_ranking,"f1_presentation","constructor_standings","race_year")

# COMMAND ----------

merge_condition = "tab.race_year = res.race_year AND tab.team = res.team"
merge_delta_data(final_ranking, "f1_presentation", "constructor_standings", presentation_folder, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC f1_presentation.constructor_standings

# COMMAND ----------


