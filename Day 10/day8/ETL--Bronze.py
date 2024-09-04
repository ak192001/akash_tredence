# Databricks notebook source
# MAGIC %run
# MAGIC %run /Workspace/Users/akash_1724384775766@npupgradassessment.onmicrosoft.com/Day 10/day8/includes

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")
