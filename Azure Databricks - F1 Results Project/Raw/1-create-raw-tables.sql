-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits; 
create table f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) using csv
options (path "/mnt/databricks1dl11/raw/circuits.csv", header TRUE);

select * from f1_raw.circuits;

-- COMMAND ----------

drop table if exists f1_raw.races; 
create table f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) using csv
options (path "/mnt/databricks1dl11/raw/races.csv", header TRUE);

select * from f1_raw.races;

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
) using json
options(path "/mnt/databricks1dl11/raw/constructors.json");

select * from f1_raw.constructors;

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
) using json
options(path "/mnt/databricks1dl11/raw/drivers.json");

select * from f1_raw.drivers;

-- COMMAND ----------

drop table if exists f1_raw.results;
create table f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT, 
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
) using json
options(path "/mnt/databricks1dl11/raw/results.json");

select * from f1_raw.results;

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
) using json
options(path "/mnt/databricks1dl11/raw/pit_stops.json", multiLine TRUE);

select * from f1_raw.pit_stops;

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  postion INT,
  time STRING,
  milliseconds INT
) using csv
options(path "/mnt/databricks1dl11/raw/lap_times");

select * from f1_raw.lap_times;

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  postion INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
) using json
options(path "/mnt/databricks1dl11/raw/qualifying", multiLine TRUE);

select * from f1_raw.qualifying;

-- COMMAND ----------


