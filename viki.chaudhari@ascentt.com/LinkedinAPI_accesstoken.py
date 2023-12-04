# Databricks notebook source
# MAGIC %python
# MAGIC import requests
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder \
# MAGIC .appName("LinkedInDataExtraction") \
# MAGIC .getOrCreate()
# MAGIC
# MAGIC #client_id = "78v73po771ls0o"
# MAGIC
# MAGIC #client_secret = "aGsXaixDsbFCWGGg"
# MAGIC
# MAGIC access_token = "AQVW3eab17Pe20wypdA5jJxsPEMweKdVI5U7mU_2EkTFoGyTRRUV5x2_dZhjdA7V-Q2niRDWIvNqsa-fU4eYg5jZ2-eKIVVDzDNgoa-86hqypzRQg3mz9t8rl9I4g2Kjfo1bIWaoCNIKjMV3zI1zZT8CcQDM2oZDbtuRsLRtaNe8MTYW7ULe2dWvzO8ycfvRAS4JO2VEQ2m_9AvRCKuWMl21IPxG5cczGnamuI6o1KFHYK16QhvHW2xKh7DGA1rgNo2s-poWMv29FTeVY4JiS1-CZWT0RTpZjZf31tA5uTO4dv82SQYqblkK4LzjRuI-7pKGGAlCr7Z_OhSyzwXUuXAToVf8Kw"
# MAGIC
# MAGIC profile_url = "https://api.linkedin.com/v2/me"
# MAGIC
# MAGIC
# MAGIC headers = {
# MAGIC
# MAGIC "Authorization": f"Bearer {access_token}",
# MAGIC
# MAGIC "Connection": "Keep-Alive"
# MAGIC
# MAGIC }
# MAGIC
# MAGIC
# MAGIC response = requests.get(profile_url, headers=headers)
# MAGIC
# MAGIC profile_data = response.json()
# MAGIC display(profile_data)
# MAGIC
# MAGIC profile_df = spark.createDataFrame([profile_data])
# MAGIC
# MAGIC profile_df.show()
# MAGIC profile_df.printSchema()
# MAGIC

# COMMAND ----------

