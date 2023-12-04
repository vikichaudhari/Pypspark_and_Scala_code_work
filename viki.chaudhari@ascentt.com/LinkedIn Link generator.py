# Databricks notebook source
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

# Make API requests using the access token
# access_token = token_response.json()["access_token"]
search_url = "https://api.linkedin.com/v2/search/people"
query = "Data Engineer"
import requests
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

access_token = "AQVW3eab17Pe20wypdA5jJxsPEMweKdVI5U7mU_2EkTFoGyTRRUV5x2_dZhjdA7V-Q2niRDWIvNqsa-fU4eYg5jZ2-eKIVVDzDNgoa-86hqypzRQg3mz9t8rl9I4g2Kjfo1bIWaoCNIKjMV3zI1zZT8CcQDM2oZDbtuRsLRtaNe8MTYW7ULe2dWvzO8ycfvRAS4JO2VEQ2m_9AvRCKuWMl21IPxG5cczGnamuI6o1KFHYK16QhvHW2xKh7DGA1rgNo2s-poWMv29FTeVY4JiS1-CZWT0RTpZjZf31tA5uTO4dv82SQYqblkK4LzjRuI-7pKGGAlCr7Z_OhSyzwXUuXAToVf8Kw"

# COMMAND ----------

