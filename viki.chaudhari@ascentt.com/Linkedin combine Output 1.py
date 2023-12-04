# Databricks notebook source
from pyspark.sql.functions import collect_list
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

# Iterate through the list of DataFrames and union them
for df in result_dataframes[1:]:
    df1 = df1.unionAll(df)

# Show the resulting combined DataFrame
display(df1)


# COMMAND ----------

from pyspark.sql import SparkSession

df_selected = df1.select("full_name","education_activities_and_societies","education_degree_name", "education_ends_at")

final_selected = final.drop("education")

# Perform the join operation on the "full_name" column
combined_df = df_selected.join(final_selected, on=["full_name"], how="inner")

# Show the resulting combined DataFrame
final_selected=combined_df
display(combined_df)



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

    json_col_df = json_col_df.select(
        col("full_name"),
        col("company").alias("experience_company"),
        col("company_linkedin_profile_url").alias("experience_company_linkedin_profile_url"),
        col("description").alias("experience_description"),
        col("starts_at").alias("experience_starts_at"),
        col("ends_at").alias("experience_ends_at"),
        col("location").alias("experience_location"),
        col("logo_url").alias("experience_logo_url"),
        col("title").alias("experience_title")
    )

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)
    
df1 = result_dataframes[0]
# Iterate through the list of DataFrames and union them
for df in result_dataframes[1:]:
    df1 = df1.unionAll(df)

# Show the resulting combined DataFrame
display(df1)


# COMMAND ----------

from pyspark.sql import SparkSession

# Select specific columns from df
df_selected = df1.select("full_name","experience_title","experience_company", "experience_description","experience_starts_at","experience_ends_at")

# Drop the "education" column from final
final_selected = final_selected.drop("experiences")

# Perform the join operation on the "full_name" column
combined_df = df_selected.join(final_selected, on=["full_name"], how="inner")

# Show the resulting combined DataFrame
display(combined_df)



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
    col("full_name"),
    col("authority").alias("certification_authority"),
    col("display_source").alias("certification_display_source"),
    col("ends_at").alias("certification_ends_at"),
    col("license_number").alias("certification_license_number"),
    col("name").alias("certification_name"),
    col("starts_at").alias("certification_starts_at"),
    col("url").alias("certification_url")
)

    # Append the result DataFrame to the list
    result_dataframes.append(json_col_df)

df1 = result_dataframes[0]
# Iterate through the list of DataFrames and union them
for df in result_dataframes[1:]:
    df1 = df1.unionAll(df)

# Show the resulting combined DataFrame
display(df1)



# COMMAND ----------

from pyspark.sql import SparkSession

# Select specific columns from df
df_selected = df1.select("full_name","certification_name","certification_license_number", "certification_authority")

# Drop the "education" column from final
final_selected = final_selected.drop("certifications")

# Perform the join operation on the "full_name" column
combined_df = df_selected.join(final_selected, on=["full_name"], how="inner")

# Show the resulting combined DataFrame
display(combined_df)



# COMMAND ----------

