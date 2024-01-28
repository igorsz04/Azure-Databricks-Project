-- Databricks notebook source
SELECT driver_name, count(1) as total_races, sum(calculated_points)/count(1) as avg_points, sum(calculated_points) as total_points FROM f1_presentation.calculated_race_results
WHERE race_year > 2000
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name, count(1) as total_races, sum(calculated_points)/count(1) as avg_points, sum(calculated_points) as total_points 
FROM f1_presentation.calculated_race_results
WHERE race_year > 2000
GROUP BY team_name 
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


