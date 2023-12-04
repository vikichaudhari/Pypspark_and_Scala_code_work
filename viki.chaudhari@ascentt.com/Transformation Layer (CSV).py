# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC file1 = spark.read.format("csv").load('dbfs:/FileStore/tables/data.csv', header=True)
# MAGIC display(file1)

# COMMAND ----------

