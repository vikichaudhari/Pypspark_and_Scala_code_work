# Databricks notebook source
file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output2.parquet')
display(file)


# COMMAND ----------

final=file.select(['full_name', 'gender', 'birth_date', 'personal_emails', 'personal_numbers', 'education', 'country_full_name', 'state','city', 'experiences',  'headline',
'industry', 'skills', 'summary', 'certifications', 'accomplishment_organisations', 'accomplishment_projects', 'accomplishment_honors_awards', 'inferred_salary', 
'accomplishment_publications'])

display(final)

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
    from pyspark.sql.functions import col

    json_col_df = json_col_df.select(
        col("full_name").alias("full_name"),
        col("activities_and_societies").alias("education_activities_and_societies"),
        col("degree_name").alias("education_degree_name"),
        col("description").alias("education_description"),
        col("starts_at").alias("education_starts_at"),
        col("ends_at").alias("education_ends_at"),
        col("field_of_study").alias("education_field_of_study"),
        col("grade").alias("education_grade"),
        col("logo_url").alias("education_logo_url"),
        col("school").alias("education_school"),
        col("school_linkedin_profile_url").alias("education_school_linkedin_profile_url") 
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

df1 = result_dataframes[0]  # Initialize with the first DataFrame

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
