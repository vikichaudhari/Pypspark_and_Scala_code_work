# Databricks notebook source
import requests
import time  # Import the time module to use the sleep function

# Define your Google Custom Search API key and search engine ID
api_key = 'AIzaSyBC4aS_LdLkmslHiwa_98Xl_EhDa2BHEms'
search_engine_id = 'e79bf635bd4974ad0'

keyword = "Data Engineer"
place="Chennai"
experience = "5+ years Experience"
links = []

results_per_page = 10
total_results = 10
num_pages = (total_results + results_per_page - 1) // results_per_page

for page in range(num_pages):
    start_index = page * results_per_page

    # Define the URL for the Google Custom Search API
    url = f'https://www.googleapis.com/customsearch/v1?key={api_key}&cx={search_engine_id}&q={keyword}+{place}+{experience}+-intitle:"profiles"+-inurl:"dir/"+site:in.linkedin.com/in/+OR+site:in.linkedin.com/pub/&as_oq=bachelor+degree+licence&start={start_index}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        for item in data.get('items', []):
            link = item.get('link')
            if link and '//in.linkedin.com/in/' in link:
                links.append(link)
        
        # Introduce a delay of 1 second between requests to avoid exceeding the rate limit
        time.sleep(1)
    elif response.status_code == 429:
        # If you get a 429 status code, you're making requests too quickly, so introduce a longer delay
        time.sleep(10)  # Sleep for 10 seconds and then retry
    else:
        print(f"Failed to retrieve page {page + 1}. Status code: {response.status_code}")

print("Collected LinkedIn profile links:")
print(links)
