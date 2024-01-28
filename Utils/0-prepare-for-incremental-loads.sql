-- Databricks notebook source
drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed 
location "/mnt/databricks1dl11/processed"

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_presentation 
location "/mnt/databricks1dl11/presentation"

-- COMMAND ----------


