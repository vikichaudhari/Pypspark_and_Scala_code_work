# Databricks notebook source
# MAGIC %python
# MAGIC file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output.parquet')
# MAGIC display(file)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
#display(file)
json_exploded=file.select(explode(file.accomplishment_projects))

json_col=json_exploded.select(json_exploded.col)
json_col_df = json_col.select("col.description", "col.starts_at", "col.title", "col.url")
display(json_col_df)

# COMMAND ----------

json_exploded = file.select(explode(file.accomplishment_publications).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.description",
    "col.name",  
    "col.published_on",
    "col.publisher",
    "col.url"
)

# Display the resulting DataFrame
display(json_col_df)


# COMMAND ----------

json_exploded = file.select(explode(file.accomplishment_test_scores
).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.date_on",
    "col.description",
    "col.name",
    "col.score"
)

# Display the resulting DataFrame
display(json_col_df)




# COMMAND ----------

json_exploded = file.select(explode(file.certifications).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.authority",
    "col.display_source",
    "col.ends_at",
    "col.license_number",
    "col.name",
    "col.starts_at",
    "col.url"
)

# Display the resulting DataFrame
display(json_col_df)

# COMMAND ----------

json_exploded = file.select(explode(file.education).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.activities_and_societies",
    "col.degree_name",
    "col.description",
    "col.ends_at",
    "col.field_of_study",
    "col.grade",
    "col.logo_url",
    "col.school",
    "col.school_linkedin_profile_url",
    "col.starts_at"
)

# Display the resulting DataFrame
display(json_col_df)

# COMMAND ----------

json_exploded = file.select(explode(file.experiences).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.company",
    "col.company_linkedin_profile_url",
    "col.description",
    "col.ends_at",
    "col.location",
    "col.logo_url",
    "col.starts_at",
    "col.title"
)

# Display the resulting DataFrame
display(json_col_df)


# COMMAND ----------

json_exploded = file.select(explode(file.people_also_viewed).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.link",
    "col.location",
    "col.name",
    "col.summary"
)

# Display the resulting DataFrame
display(json_col_df)

# COMMAND ----------

json_exploded = file.select(explode(file.similarly_named_profiles).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.link",
    "col.location",
    "col.name",
    "col.summary"
)

# Display the resulting DataFrame
display(json_col_df)

# COMMAND ----------

json_exploded = file.select(explode(file.volunteer_work).alias("col"))

# Select the desired fields from the exploded DataFrame
json_col_df = json_exploded.select(
    "col.cause",
    "col.company",
    "col.company_linkedin_profile_url",
    "col.description",
    "col.ends_at",
    "col.logo_url",
    "col.starts_at",
    "col.title"
)

display(json_col_df)

# COMMAND ----------

