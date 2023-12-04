# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC import requests
# MAGIC import json
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder \
# MAGIC .appName("LinkedInDataExtraction") \
# MAGIC .getOrCreate()
# MAGIC
# MAGIC PROXYCURL_API_KEY = 'Aktb-ttvn0CHW1tG-jtR-A'
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
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC # Convert the JSON data to a DataFrame
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC r1=json.dumps(result)
# MAGIC df = spark.read.json(spark.sparkContext.parallelize([r1]))
# MAGIC
# MAGIC # df.show() uncomment if you wish to debug the DataFrame
# MAGIC
# MAGIC df1 = df \
# MAGIC     .withColumn('accomplishment_courses', col('accomplishment_courses').cast('string')) \
# MAGIC     .withColumn('accomplishment_honors_awards', col('accomplishment_honors_awards').cast('string')) \
# MAGIC     .withColumn('accomplishment_organisations', col('accomplishment_organisations').cast('string'))\
# MAGIC     .withColumn('accomplishment_patents', col('accomplishment_patents').cast('string'))\
# MAGIC     .withColumn('accomplishment_projects', col('accomplishment_projects').cast('string'))\
# MAGIC     .withColumn('accomplishment_publications', col('accomplishment_publications').cast('string'))\
# MAGIC     .withColumn('accomplishment_test_scores', col('accomplishment_test_scores').cast('string'))\
# MAGIC     .withColumn('activities', col('activities').cast('string'))\
# MAGIC     .withColumn('articles', col('articles').cast('string'))\
# MAGIC     .withColumn('education', col('education').cast('string'))\
# MAGIC     .withColumn('certifications', col('certifications').cast('string'))\
# MAGIC     .withColumn('experiences', col('experiences').cast('string'))\
# MAGIC     .withColumn('people_also_viewed', col('people_also_viewed').cast('string'))\
# MAGIC     .withColumn('profile_pic_url', col('profile_pic_url').cast('string'))\
# MAGIC     .withColumn('similarly_named_profiles', col('similarly_named_profiles').cast('string'))\
# MAGIC     .withColumn('volunteer_work', col('volunteer_work').cast('string'))\
# MAGIC     .withColumn('languages', col('languages').cast('string'))\
# MAGIC     .withColumn('groups', col('groups').cast('string'))\
# MAGIC     .withColumn('interests', col('interests').cast('string'))\
# MAGIC     .withColumn('personal_emails', col('personal_emails').cast('string'))\
# MAGIC     .withColumn('personal_numbers', col('personal_numbers').cast('string'))\
# MAGIC     .withColumn('recommendations', col('recommendations').cast('string'))\
# MAGIC     .withColumn('skills', col('skills').cast('string'))
# MAGIC
# MAGIC display(df1)
# MAGIC
# MAGIC df1.repartition(1).write.csv('dbfs:/FileStore/tables/data.csv', header=True, mode='overwrite')
# MAGIC print("CSV file has been saved successfully in Databricks storage.")
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/data.csv'))

# COMMAND ----------

