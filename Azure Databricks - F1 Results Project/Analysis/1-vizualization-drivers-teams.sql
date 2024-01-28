-- Databricks notebook source
-- MAGIC %python 
-- MAGIC html = """<h1 style="color:DarkBlue;text-align:center;font-family:Stencil">Best teams and drivers in Formula One</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers as
SELECT driver_name, count(1) as total_races, sum(calculated_points)/count(1) as avg_points, sum(calculated_points) as total_points,
RANK() OVER(ORDER BY sum(calculated_points)/count(1) DESC) as total_rank
FROM f1_presentation.calculated_race_results
WHERE race_year > 2000
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, driver_name, count(1) as total_races, sum(calculated_points)/count(1) as avg_points, sum(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_drivers WHERE total_rank<10)
GROUP BY race_year, driver_name 
ORDER BY avg_points desc

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams as
SELECT team_name, count(1) as total_races, sum(calculated_points)/count(1) as avg_points, sum(calculated_points) as total_points,
RANK() OVER(ORDER BY sum(calculated_points)/count(1) DESC) as total_rank 
FROM f1_presentation.calculated_race_results
WHERE race_year > 2000
GROUP BY team_name 
HAVING total_races >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, team_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name from v_dominant_teams WHERE total_rank<=8)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points desc

-- COMMAND ----------

--DASHBOARD
https://adb-4783817378184347.7.azuredatabricks.net/?o=4783817378184347#notebook/3466177935356183/dashboard/3466177935356195/present
