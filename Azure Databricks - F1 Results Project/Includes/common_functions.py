# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def partition_column_change(input, partition_column):
    column_list = []
    for column_name in input.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_data = input.select(column_list)
    return output_data

# COMMAND ----------

def overwrite_partition(input, db_name, table_name, partition_column):
   output_data = partition_column_change(input, partition_column) 

   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
       output_data.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
   else:
       output_data.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

def merge_delta_data(input, db_name, table, folder, merge_condition, partition_col):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder}/{table}")
        deltaTable.alias("tab").merge(
            input.alias("res"),
            merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table}")


# COMMAND ----------


