# Databricks notebook source
# MAGIC %run /Users/viki.chaudhari@ascentt.com/Linkedin_Links

# COMMAND ----------


import requests
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("LinkedInDataExtraction") \
.getOrCreate()

#PROXYCURL_API_KEY = 'AoBWlmXFEU0o9dttMU5o-g'

def get_profile(params):
    api_endpoint = 'https://nubela.co/proxycurl/api/v2/linkedin'
    header_dic = {'Authorization': 'Bearer ' + PROXYCURL_API_KEY}
    response = requests.get(api_endpoint,
                            params=params,
                            headers=header_dic)
    return response.json()

json_responses=[]
for link in links:
    params = {
        'url': link,
    }
    json_responses.append(get_profile(params))
print(json_responses)


# COMMAND ----------

#Parquet
r1=json.dumps(json_responses)
df= spark.read.json(spark.sparkContext.parallelize([r1]))

df.repartition(1).write.format("parquet").save("dbfs:/FileStore/tables/output2.parquet",header=True,mode="overwrite")
print("Paequet file has been saved successfully in Databricks storage.")



# COMMAND ----------

file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output2.parquet')
display(file)

# COMMAND ----------

file = file.select(["full_name","accomplishment_courses", "accomplishment_honors_awards", "accomplishment_organisations", "accomplishment_patents", "accomplishment_projects", "accomplishment_publications", "accomplishment_test_scores", "activities", "articles", "background_cover_image_url", "birth_date", "certifications", "city", "connections", "country", "country_full_name", "education", "experiences", "extra", "first_name", "follower_count", "gender", "groups", "headline", "industry", "inferred_salary", "interests", "languages", "last_name", "occupation", "people_also_viewed", "personal_emails", "personal_numbers", "profile_pic_url", "public_identifier", "recommendations", "similarly_named_profiles", "skills", "state", "summary", "volunteer_work"])

display(file)

# COMMAND ----------

names=file.select("full_name")
display(names)

# COMMAND ----------

from pyspark.sql.functions import *

json_exploded=file.select(explode(file.accomplishment_projects))
json_col_df = json_col.select("col.description", "col.starts_at", "col.title", "col.url")
df = json_col_df.withColumn("day", col("starts_at.day")) \
    .withColumn("month", col("starts_at.month")) \
    .withColumn("year", col("starts_at.year"))

# Create a new column with the regular date format
df = df.withColumn("starts_at", concat_ws("-",
    col("year"), col("month"), col("day")
))
json_col_df = df.select("description", "starts_at", "title", "url")
display(json_col_df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)
    
    # Explode and select columns
    json_exploded = filtered_df.select(explode(filtered_df.accomplishment_projects))
    json_col_df = json_exploded.select("col.description", "col.starts_at", "col.title", "col.url")
    
    # Extract day, month, and year components and create a new date column
    df = json_col_df.withColumn("day", col("starts_at.day")) \
        .withColumn("month", col("starts_at.month")) \
        .withColumn("year", col("starts_at.year"))
    
    # Create a new column with the regular date format
    df = df.withColumn("starts_at", concat_ws("-",
         col("year"), col("month"), col("day")
    ))
    
    # Add the full_name column to the DataFrame
    df = df.withColumn("full_name", lit(name))
    
    # Select the desired columns for the final DataFrame
    json_col_df = df.select("full_name", "description", "starts_at", "title", "url")
    
    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "accomplishment_publications" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.accomplishment_publications))
    json_col = json_exploded.select(json_exploded.col)

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_col.select(
        "col.description",
        "col.name",
        "col.published_on",
        "col.publisher",
        "col.url"
    )

    # Create a new column with the regular date format
    json_col_df = json_col_df.withColumn("published_day", col("published_on.day")) \
        .withColumn("published_month", col("published_on.month")) \
        .withColumn("published_year", col("published_on.year"))

    # Concatenate the date components into a new "published_on" column
    json_col_df = json_col_df.withColumn("published_on", concat_ws("-",
        col("published_year"), col("published_month"), col("published_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select("full_name", "description", "published_on", "name", "publisher", "url")

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "accomplishment_test_scores" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.accomplishment_test_scores))
    json_col = json_exploded.select(json_exploded.col)

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_col.select(
        "col.date_on",
        "col.description",
        "col.name",
        "col.score"
    )

    # Create a new column with the regular date format
    json_col_df = json_col_df.withColumn("test_date_day", col("date_on.day")) \
        .withColumn("test_date_month", col("date_on.month")) \
        .withColumn("test_date_year", col("date_on.year"))

    # Concatenate the date components into a new "date_on" column
    json_col_df = json_col_df.withColumn("date_on", concat_ws("-",
        col("test_date_year"), col("test_date_month"), col("test_date_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select("full_name", "date_on", "description", "name", "score")

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "certifications" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.certifications).alias("col"))

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

    # Create new columns with the regular date format for starts_at and ends_at
    json_col_df = json_col_df.withColumn("starts_at_day", col("starts_at.day")) \
        .withColumn("starts_at_month", col("starts_at.month")) \
        .withColumn("starts_at_year", col("starts_at.year")) \
        .withColumn("ends_at_day", col("ends_at.day")) \
        .withColumn("ends_at_month", col("ends_at.month")) \
        .withColumn("ends_at_year", col("ends_at.year"))

    # Concatenate the date components into new "starts_at" and "ends_at" columns
    json_col_df = json_col_df.withColumn("starts_at", concat_ws("-",
        col("starts_at_year"), col("starts_at_month"), col("starts_at_day")
    )) \
        .withColumn("ends_at", concat_ws("-",
            col("ends_at_year"), col("ends_at_month"), col("ends_at_day")
        ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "authority",
        "display_source",
        "ends_at",
        "license_number",
        "name",
        "starts_at",
        "url"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "education" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.education).alias("col"))

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

    # Create new columns with the regular date format for "starts_at" and "ends_at"
    json_col_df = json_col_df.withColumn("start_date_day", col("starts_at.day")) \
        .withColumn("start_date_month", col("starts_at.month")) \
        .withColumn("start_date_year", col("starts_at.year"))

    json_col_df = json_col_df.withColumn("end_date_day", col("ends_at.day")) \
        .withColumn("end_date_month", col("ends_at.month")) \
        .withColumn("end_date_year", col("ends_at.year"))

    # Concatenate the date components into new "starts_at" and "ends_at" columns
    json_col_df = json_col_df.withColumn("starts_at", concat_ws("-",
        col("start_date_year"), col("start_date_month"), col("start_date_day")
    ))

    json_col_df = json_col_df.withColumn("ends_at", concat_ws("-",
        col("end_date_year"), col("end_date_month"), col("end_date_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "activities_and_societies",
        "degree_name",
        "description",
        "starts_at",
        "ends_at",
        "field_of_study",
        "grade",
        "logo_url",
        "school",
        "school_linkedin_profile_url"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "accomplishment_honors_awards" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.accomplishment_honors_awards).alias("col"))

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_exploded.select(
        "col.description",
        "col.issued_on",
        "col.issuer",
        "col.title"
    )

    # Create new columns with the regular date format for "issued_on"
    json_col_df = json_col_df.withColumn("issued_on_day", col("issued_on.day")) \
        .withColumn("issued_on_month", col("issued_on.month")) \
        .withColumn("issued_on_year", col("issued_on.year"))

    # Concatenate the date components into the "issued_on" column
    json_col_df = json_col_df.withColumn("issued_on", concat_ws("-",
        col("issued_on_year"), col("issued_on_month"), col("issued_on_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "description",
        "issued_on",
        "issuer",
        "title"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "experiences" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.experiences).alias("col"))

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

    # Create new columns with the regular date format for "starts_at" and "ends_at"
    json_col_df = json_col_df.withColumn("start_date_day", col("starts_at.day")) \
        .withColumn("start_date_month", col("starts_at.month")) \
        .withColumn("start_date_year", col("starts_at.year"))

    json_col_df = json_col_df.withColumn("end_date_day", col("ends_at.day")) \
        .withColumn("end_date_month", col("ends_at.month")) \
        .withColumn("end_date_year", col("ends_at.year"))

    # Concatenate the date components into new "starts_at" and "ends_at" columns
    json_col_df = json_col_df.withColumn("starts_at", concat_ws("-",
        col("start_date_year"), col("start_date_month"), col("start_date_day")
    ))

    json_col_df = json_col_df.withColumn("ends_at", concat_ws("-",
        col("end_date_year"), col("end_date_month"), col("end_date_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "company",
        "company_linkedin_profile_url",
        "description",
        "starts_at",
        "ends_at",
        "location",
        "logo_url",
        "title"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, explode, lit
from pyspark.sql.functions import concat_ws

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "people_also_viewed" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.people_also_viewed).alias("col"))

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_exploded.select(
        "col.link",
        "col.location",
        "col.name",
        "col.summary"
    )

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "link",
        "location",
        "name",
        "summary"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, explode, lit
from pyspark.sql.functions import concat_ws

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "similarly_named_profiles" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.similarly_named_profiles).alias("col"))

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_exploded.select(
        "col.link",
        "col.location",
        "col.name",
        "col.summary"
    )

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "link",
        "location",
        "name",
        "summary"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "volunteer_work" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.volunteer_work).alias("col"))

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

    # Create new columns with the regular date format for "starts_at" and "ends_at"
    json_col_df = json_col_df.withColumn("start_date_day", col("starts_at.day")) \
        .withColumn("start_date_month", col("starts_at.month")) \
        .withColumn("start_date_year", col("starts_at.year"))

    json_col_df = json_col_df.withColumn("end_date_day", col("ends_at.day")) \
        .withColumn("end_date_month", col("ends_at.month")) \
        .withColumn("end_date_year", col("ends_at.year"))

    # Concatenate the date components into new "starts_at" and "ends_at" columns
    json_col_df = json_col_df.withColumn("starts_at", concat_ws("-",
        col("start_date_year"), col("start_date_month"), col("start_date_day")
    ))

    json_col_df = json_col_df.withColumn("ends_at", concat_ws("-",
        col("end_date_year"), col("end_date_month"), col("end_date_day")
    ))

    # Add the full_name column to the DataFrame
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "cause",
        "company",
        "company_linkedin_profile_url",
        "description",
        "starts_at",
        "ends_at",
        "logo_url",
        "title"
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode

# Assuming you have a list of names
# Select the "full_name" column from the DataFrame
names_df = file.select("full_name")
names_list = [row.full_name for row in names_df.collect()]

# Create an empty list to store the resulting DataFrames
result_dataframes = []

for name in names_list:
    # Filter the DataFrame to get the row for the desired full_name
    filtered_df = file.filter(file["full_name"] == name)

    # Explode the "volunteer_work" column and alias it as "col"
    json_exploded = filtered_df.select(explode(filtered_df.accomplishment_courses).alias("col"))

    # Select the desired fields from the exploded DataFrame
    json_col_df = json_exploded.select(
        "col.name",
        "col.number"
    )
    json_col_df = json_col_df.withColumn("full_name", lit(name))

    # Select the desired columns
    json_col_df = json_col_df.select(
        "full_name",
        "name",
        "number",
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

# Display the list of resulting DataFrames
for df in result_dataframes:
    display(df)


# COMMAND ----------

