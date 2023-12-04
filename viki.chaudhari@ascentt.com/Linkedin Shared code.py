# Databricks notebook source
# MAGIC %run /Users/viki.chaudhari@ascentt.com/Linkedin_Links

# COMMAND ----------

from pyspark.sql.functions import collect_list
file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output2.parquet')
display(file)

# COMMAND ----------

final=file.select(['full_name', 'gender', 'birth_date', 'personal_emails', 'personal_numbers', 'education', 'country_full_name', 'state','city', 'experiences',  'headline',
'industry', 'skills', 'summary', 'certifications', 'accomplishment_organisations', 'accomplishment_projects', 'accomplishment_honors_awards'])

display(final)

# COMMAND ----------

final_result=final_selected.select(['full_name', 'gender', 'birth_date', 'personal_emails', 'personal_numbers',  'country_full_name', 'state', 'city', 'experience_title', 'experience_company', 'experience_starts_at', 'experience_ends_at', 'experience_description', 'industry',  'skills',  'summary', 'headline', 'education_field_of_study', 'education_degree_name', 'education_ends_at',  'certification_name', 'certification_license_number', 'certification_authority', 'certification_url', 'accomplishment_projects_description', 'accomplishment_honors_awards_description', 'accomplishment_honors_awards_title', 'accomplishment_honors_awards_issuer'])

display(final_result)

# COMMAND ----------

