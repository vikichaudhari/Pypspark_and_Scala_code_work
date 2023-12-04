# Databricks notebook source
import requests
from pyspark.sql.types import StringType

# Define your API credentials
client_id = "78v73po771ls0o"
client_secret = "aGsXaixDsbFCWGGg"

access_token = "AQVW3eab17Pe20wypdA5jJxsPEMweKdVI5U7mU_2EkTFoGyTRRUV5x2_dZhjdA7V-Q2niRDWIvNqsa-fU4eYg5jZ2-eKIVVDzDNgoa-86hqypzRQg3mz9t8rl9I4g2Kjfo1bIWaoCNIKjMV3zI1zZT8CcQDM2oZDbtuRsLRtaNe8MTYW7ULe2dWvzO8ycfvRAS4JO2VEQ2m_9AvRCKuWMl21IPxG5cczGnamuI6o1KFHYK16QhvHW2xKh7DGA1rgNo2s-poWMv29FTeVY4JiS1-CZWT0RTpZjZf31tA5uTO4dv82SQYqblkK4LzjRuI-7pKGGAlCr7Z_OhSyzwXUuXAToVf8Kw"
# Make API requests using the access token
search_url = "https://api.linkedin.com/v2/search/people"
query = "Data Engineer"
params = {
    "q": query,
    "start": 0,
    "count": 10
}

headers = {
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(search_url, params=params, headers=headers)
data = response.json()

# Process the data and extract profile links
profile_links = [result["publicProfileUrl"] for result in data.get("elements", [])]

# Store profile links in a Parquet file using PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LinkedInScraper").getOrCreate()
df = spark.createDataFrame(profile_links, StringType())

# Save the DataFrame as a Parquet file
df.write.mode("overwrite").parquet("linkedin_profile_links.parquet")

# Stop the Spark session
spark.stop()


# COMMAND ----------

import requests
from pyspark.sql.types import StringType

access_token = "q2QCUWBB2nqNnKpE9jmb5A"
# Make API requests using the access token
search_url = 'https://nubela.co/proxycurl/api/search/person/'

params = {
    'country': 'US',
    'page_size':'1',
    'enrich_profiles':'skip',
   'current_role_title':'Data Engineer'}

headers = {
    "Authorization": 'Bearer ' +access_token}

response = requests.get(search_url, params=params, headers=headers)
data = response.json()
print(data)


# COMMAND ----------

import requests
from pyspark.sql.types import StringType
client_id = "78mtob0z4a0qo8"
client_secret = "2sI1qxGENNZwqQGK"

# Obtain an access token using client credentials grant
token_url = "https://www.linkedin.com/oauth/v2/accessToken"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}

token_response = requests.post(token_url, data=token_data)
access_token = token_response.json()["access_token"]

# Make API requests using the access token
search_url = "https://api.linkedin.com/v2/search/people"
query = "Data Engineer"
params = {
    "q": query,
    "start": 0,
    "count": 10
}
headers = {
    "Authorization": f"Bearer {access_token}",
    "Connection": "Keep-Alive"
}

response = requests.get(search_url, params=params, headers=headers)
data = response.json()

# Process the data and extract profile links
profile_links = [result["publicProfileUrl"] for result in data.get("elements", [])]
print(data)

# COMMAND ----------

import requests
from pyspark.sql.types import StringType
client_id = "78mtob0z4a0qo8"
client_secret = "2sI1qxGENNZwqQGK"

# Obtain an access token using client credentials grant
token_url = "https://www.linkedin.com/oauth/v2/accessToken"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}

token_response = requests.post(token_url, data=token_data)
print(token_response.content)

# COMMAND ----------

AQXs3ubDWuTemYUD350mmMXU3H-1JcItEIT3bJrYk1QIyKIioml97_z2IA2zgi5Nf2CXnz5LABYGWcxEo4k7LGXrc0uEmu13FVBXnv9USR2neM-1i3IkU4F9LX0WqngxIggViEa0S_nxMM8WhVmowq67vyL4jMZxf6YkzsXtYIvAUMVD506o4mWVC_mEKlmiADT0ES0W63Bht-sqVzvVz-RJKjoOIq2LeZHEZHbCcmbb2N2CY4tGRjbrwNqbp4KQJ4a-f7SWq6nyzDnNcQ62avytGHFfvdFizUYDoKME0KUKBpZZ36FOsQlnXMVkb5J1q9Soa9-i3edHgoKMcLQHMlPWlCUwAg

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# Define your LinkedIn access token
access_token = "AQVW3eab17Pe20wypdA5jJxsPEMweKdVI5U7mU_2EkTFoGyTRRUV5x2_dZhjdA7V-Q2niRDWIvNqsa-fU4eYg5jZ2-eKIVVDzDNgoa-86hqypzRQg3mz9t8rl9I4g2Kjfo1bIWaoCNIKjMV3zI1zZT8CcQDM2oZDbtuRsLRtaNe8MTYW7ULe2dWvzO8ycfvRAS4JO2VEQ2m_9AvRCKuWMl21IPxG5cczGnamuI6o1KFHYK16QhvHW2xKh7DGA1rgNo2s-poWMv29FTeVY4JiS1-CZWT0RTpZjZf31tA5uTO4dv82SQYqblkK4LzjRuI-7pKGGAlCr7Z_OhSyzwXUuXAToVf8Kw"

# Make API requests using the access token
search_url = "https://api.linkedin.com/v2/search/people"
query = "data"
params = {
    "q": query,
    "start": 0,
    "count": 10
}

headers = {
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(search_url, params=params, headers=headers)
data = response.json()

print(data)

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("LinkedInScraper").getOrCreate()

# Define the URLs for LinkedIn search results
search_url = "https://www.linkedin.com/search/results/people/?keywords=DataEngineeer"

# Send a GET request to LinkedIn and parse the HTML
response = requests.get(search_url)
soup = BeautifulSoup(response.text, "html.parser")

# Find and extract profile links from the search results
profile_links = []
results = soup.find_all("li", class_="search-result")
for result in results:
    link = result.find("a", class_="search-result__result-link")["href"]
    # Make sure it's a valid LinkedIn profile link
    if "/in/" in link:
        profile_links.append("https://www.linkedin.com" + link)

# Create a PySpark DataFrame from the profile links
df = spark.createDataFrame(profile_links, StringType()).withColumnRenamed("value", "LinkedIn_Profile_Link")

# Show the top 10 profile links
top_10_links = df.limit(10)
top_10_links.show(truncate=False)




# COMMAND ----------

import requests
from bs4 import BeautifulSoup

# Define the LinkedIn search URL with your query
search_url = "https://www.linkedin.com/search/results/people/?keywords=aws"

# Send a GET request to LinkedIn
response = requests.get(search_url)

# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    print(soup)
    # Parse and extract profile links or other data as needed
    # Note that LinkedIn's website structure may change over time, so this code may need adjustments.
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")

# Be sure to respect LinkedIn's robots.txt and terms of service, and do not overload their servers with requests.


# COMMAND ----------

from pyspark.sql import SparkSession
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

# Initialize a Spark session
spark = SparkSession.builder.appName("LinkedInScraper").getOrCreate()

# Define the search keyword
search_keyword = "data"

# Set up a Selenium WebDriver (make sure you have the Chrome WebDriver installed)
driver = webdriver.Chrome()

# Open LinkedIn and perform a search
driver.get("https://www.linkedin.com/")
time.sleep(2)  # Wait for the page to load

# Find the search bar and enter the keyword
search_bar = driver.find_element_by_name("keywords")
search_bar.send_keys(search_keyword)
search_bar.send_keys(Keys.RETURN)

# Wait for search results to load (you may need to adjust the waiting time)
time.sleep(5)

# Find and extract profile links
profile_links = []

# Loop through search results and extract profile links
results = driver.find_elements_by_class_name("search-result__info")
for result in results:
    try:
        link = result.find_element_by_tag_name("a").get_attribute("href")
        if "/in/" in link:
            profile_links.append(link)
    except:
        pass

# Create a PySpark DataFrame from the profile links
df = spark.createDataFrame(profile_links, "string").withColumnRenamed("value", "LinkedIn_Profile_Link")

# Show the top 10 profile links
top_10_links = df.limit(10)
top_10_links.show(truncate=False)

# Close the Selenium WebDriver
driver.quit()

# Stop the Spark session
spark.stop()


# COMMAND ----------

