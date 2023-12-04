# Databricks notebook source
# MAGIC %python
# MAGIC import requests
# MAGIC import json
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder \
# MAGIC .appName("LinkedInDataExtraction") \
# MAGIC .getOrCreate()
# MAGIC
# MAGIC PROXYCURL_API_KEY = 'joR50XA78vvEA7ku8rt0BQ'
# MAGIC
# MAGIC
# MAGIC def get_profile():
# MAGIC     api_endpoint = 'https://nubela.co/proxycurl/api/v2/linkedin'
# MAGIC     header_dic = {'Authorization': 'Bearer ' + PROXYCURL_API_KEY}
# MAGIC     params = {
# MAGIC         'url': f'https://www.linkedin.com/in/aditya-hicounselor/',
# MAGIC     }
# MAGIC     response = requests.get(api_endpoint,
# MAGIC                             params=params,
# MAGIC                             headers=header_dic)
# MAGIC     return response.json()
# MAGIC
# MAGIC result=get_profile()

# COMMAND ----------

print(result)

# COMMAND ----------

# MAGIC %python
# MAGIC #Parquet
# MAGIC r1=json.dumps(result)
# MAGIC df= spark.read.json(spark.sparkContext.parallelize([r1]))
# MAGIC
# MAGIC df.repartition(1).write.format("parquet").save("dbfs:/FileStore/tables/output.parquet",header=True,mode="overwrite")
# MAGIC print("Paequet file has been saved successfully in Databricks storage.")
# MAGIC
# MAGIC file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output.parquet')
# MAGIC display(file)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/output.parquet'))

# COMMAND ----------

