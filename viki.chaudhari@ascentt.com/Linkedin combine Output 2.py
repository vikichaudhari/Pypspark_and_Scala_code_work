# Databricks notebook source
# MAGIC %run /Users/viki.chaudhari@ascentt.com/Linkedin_Links

# COMMAND ----------


import requests
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("LinkedInDataExtraction") \
.getOrCreate()

PROXYCURL_API_KEY = 'JWub2EUkaHNgVE8qSBMJcw'

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
        'skills': 'include',
    }
    json_responses.append(get_profile(params))
print(json_responses)


# COMMAND ----------

#Parquet
r1=json.dumps(json_responses)
df= spark.read.json(spark.sparkContext.parallelize([r1]))

df.repartition(1).write.format("parquet").save("dbfs:/FileStore/tables/output3.parquet",header=True,mode="overwrite")
print("Paequet file has been saved successfully in Databricks storage.")


# COMMAND ----------

from pyspark.sql.functions import collect_list
file=spark.read.format("parquet").load('dbfs:/FileStore/tables/output.parquet')
display(file)


# COMMAND ----------

final=file.select(['full_name', 'skills', ])

display(final)

# COMMAND ----------

final=file.select(['full_name','education', 'country_full_name', 'state','city', 'experiences',  'headline', 'skills', 'summary', 'certifications', 'accomplishment_organisations', 'accomplishment_projects', 'accomplishment_honors_awards'])

display(final)

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode
from pyspark.sql.utils import AnalysisException

try:
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

except AnalysisException as e:
    print("Data is not available for education")
    print(e)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

try:
    # Assuming you have a SparkSession named 'spark'
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Aggregate the columns from df1
    df_selected_aggregated = df1.groupBy("full_name").agg(
        collect_list("education_field_of_study").alias("education_field_of_study"),
        collect_list("education_degree_name").alias("education_degree_name"),
        collect_list("education_ends_at").alias("education_ends_at")
    )

    # Drop the original "education" column from final
    final_selected = final.drop("education")

    # Join the aggregated DataFrame with final_selected using "full_name" as the key
    combined_df = df_selected_aggregated.join(final_selected, on=["full_name"], how="right")

    # Show the resulting combined DataFrame
    final_selected = combined_df
    display(combined_df)

except Exception as e:
    print("Data is not available for education")
    print(e)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode
from pyspark.sql.utils import AnalysisException

try:
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

except AnalysisException as e:
    print("Data is not available for experiences")
    print(e)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

try:
    # Assuming you have a SparkSession named 'spark'
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Aggregate the columns from df1
    df_selected_aggregated = df1.groupBy("full_name").agg(
        collect_list("experience_title").alias("experience_title"),
        collect_list("experience_description").alias("experience_description"),
        collect_list("experience_company").alias("experience_company"),
        collect_list("experience_starts_at").alias("experience_starts_at"),
        collect_list("experience_starts_at").alias("experience_ends_at")
    )

    # Drop the original "experiences" column from final_selected
    final_selected = final_selected.drop("experiences")

    # Join the aggregated DataFrame with final_selected using "full_name" as the key
    combined_df = df_selected_aggregated.join(final_selected, on=["full_name"], how="right")

    final_selected = combined_df

    # Show the resulting combined DataFrame
    display(combined_df)

except Exception as e:
    print("Data is not available for experiences")
    print(e)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode
from pyspark.sql.utils import AnalysisException

try:
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

except AnalysisException as e:
    print("Data is not available for certifications ")
    print(e)  

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

try:
    # Assuming you have a SparkSession named 'spark'
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Aggregate the columns from df1
    df_selected_aggregated = df1.groupBy("full_name").agg(
        collect_list("certification_name").alias("certification_name"),
        collect_list("certification_license_number").alias("certification_license_number"),
        collect_list("certification_authority").alias("certification_authority"),
        collect_list("certification_url").alias("certification_url"),

    )

    # Drop the original "certifications" column from final_selected
    final_selected = final_selected.drop("certifications")

    # Join the aggregated DataFrame with final_selected using "full_name" as the key
    combined_df = df_selected_aggregated.join(final_selected, on=["full_name"], how="right")

    final_selected = combined_df

    # Show the resulting combined DataFrame
    display(combined_df)

except Exception as e:
    print("Data is not available for certifications ")
    print(e)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode
from pyspark.sql.utils import AnalysisException

try:
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
        
        # Select the desired columns for the final DataFrame and rename columns
        json_col_df = df.select(
            col("full_name"),
            col("description").alias("accomplishment_projects_description"),
            col("starts_at").alias("accomplishment_projects_starts_at"),
            col("title").alias("accomplishment_projects_title"),
            col("url").alias("accomplishment_projects_url")
        )
        
        # Append the result DataFrame to the list
        result_dataframes.append(json_col_df)

    df1 = result_dataframes[0]
    # Iterate through the list of DataFrames and union them
    for df in result_dataframes[1:]:
        df1 = df1.unionAll(df)

    # Show the resulting combined DataFrame
    display(df1)

except AnalysisException as e:
    print("Data is not available for accomplishment_projects")
    print(e)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

try:
    # Assuming you have a SparkSession named 'spark'
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Aggregate the "accomplishment_projects_description" column from df1
    df_selected_aggregated = df1.groupBy("full_name").agg(
        collect_list("accomplishment_projects_description").alias("accomplishment_projects_description")
    )

    # Drop the original "accomplishment_projects" column from final_selected
    final_selected = final_selected.drop("accomplishment_projects")

    # Join the aggregated DataFrame with final_selected using "full_name" as the key
    combined_df = df_selected_aggregated.join(final_selected, on=["full_name"], how="right")

    final_selected = combined_df

    # Show the resulting combined DataFrame
    display(combined_df)

except Exception as e:
    print("Data is not available for accomplishment_projects")
    print(e)

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.functions import explode
from pyspark.sql.utils import AnalysisException

try:
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
            col("full_name"),
            col("description").alias("accomplishment_honors_awards_description"),
            col("issued_on").alias("accomplishment_honors_awards_issued_on"),
            col("issuer").alias("accomplishment_honors_awards_issuer"),
            col("title").alias("accomplishment_honors_awards_title")
        )

        # Append the result DataFrame to the list
        result_dataframes.append(json_col_df)

    df1 = result_dataframes[0]
    # Iterate through the list of DataFrames and union them
    for df in result_dataframes[1:]:
        df1 = df1.unionAll(df)

    # Show the resulting combined DataFrame
    display(df1)

except AnalysisException as e:
    print("Data is not available for accomplishment_honors_awards ")
    print(e)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

try:
    # Assuming you have a SparkSession named 'spark'
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Aggregate the columns from df1
    df_selected_aggregated = df1.groupBy("full_name").agg(
        collect_list("accomplishment_honors_awards_description").alias("accomplishment_honors_awards_description"),
        collect_list("accomplishment_honors_awards_title").alias("accomplishment_honors_awards_title"),
        collect_list("accomplishment_honors_awards_issuer").alias("accomplishment_honors_awards_issuer")
    )

    # Drop the original "accomplishment_honors_awards" column from final_selected
    final_selected = final_selected.drop("accomplishment_honors_awards")

    # Join the aggregated DataFrame with final_selected using "full_name" as the key
    combined_df = df_selected_aggregated.join(final_selected, on=["full_name"], how="right")

    final_selected = combined_df

    # Show the resulting combined DataFrame
    display(combined_df)

except Exception as e:
    print("Data is not available for accomplishment_honors_awards ")
    print(e)


# COMMAND ----------

final_result=final_selected.select(['full_name', 'country_full_name', 'state', 'city', 'experience_title', 'experience_company', 'experience_starts_at', 'experience_ends_at', 'experience_description',  'skills',  'summary', 'headline', 'education_field_of_study', 'education_degree_name', 'education_ends_at',  'certification_name', 'certification_license_number', 'certification_authority', 'certification_url', 'accomplishment_projects_description', 'accomplishment_honors_awards_description', 'accomplishment_honors_awards_title', 'accomplishment_honors_awards_issuer'])


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
from pyspark.sql import Row

# Create a DataFrame from the links list
links_data = [Row(link=link) for link in links]
links_df = spark.createDataFrame(links_data)

# Add a row number to each row in the DataFrame
window_spec = Window().orderBy(lit(0))  # Creates a window specification with a constant column for ordering
final_result_with_rownum = final_result.withColumn("row_num", row_number().over(window_spec))
links_df_with_rownum = links_df.withColumn("row_num", row_number().over(window_spec))

# Join the DataFrame with links using the row number
result_df = final_result_with_rownum.join(links_df_with_rownum, final_result_with_rownum.row_num == links_df_with_rownum.row_num, "inner").drop("row_num", "row_num")


# COMMAND ----------

result_df = result_df.withColumnRenamed("link","profile_link")
final=result_df.select(['full_name','profile_link','country_full_name', 'state', 'city', 'experience_title', 'experience_company', 'experience_starts_at', 'experience_ends_at', 'experience_description',  'skills',  'summary', 'headline', 'education_field_of_study', 'education_degree_name', 'education_ends_at',  'certification_name', 'certification_license_number', 'certification_authority', 'certification_url', 'accomplishment_projects_description', 'accomplishment_honors_awards_description', 'accomplishment_honors_awards_title', 'accomplishment_honors_awards_issuer'])
display(final)

# COMMAND ----------

