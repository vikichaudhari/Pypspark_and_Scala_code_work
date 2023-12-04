# Databricks notebook source
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

options = webdriver.ChromeOptions()
options.add_argument("headless")

exe_path = ChromeDriverManager().install()
service = Service(r"dbfs:/FileStore/tables/chrome.exe")
driver = webdriver.Chrome(service=service, options=options)

driver.get("https://www.linkedin.com/login")
sleep(6)

linkedin_username = "#Linkedin Mail I'd"
linkedin_password = "#Linkedin Password"

driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[\
							1]/input").send_keys(linkedin_username)
driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[\
							2]/input").send_keys(linkedin_password)
sleep(3)
driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[\
							3]/button").click()

profiles = ['https://www.linkedin.com/in/vinayak-rai-a9b231193/',
			'https://www.linkedin.com/in/dishajindgar/',
			'https://www.linkedin.com/in/ishita-rai-28jgj/']

for i in profiles:
	driver.get(i)
	sleep(5)
	title = driver.find_element(By.XPATH,
								"//h1[@class='text-heading-xlarge inline t-24 v-align-middle break-words']").text
	print(title)
	description = driver.find_element(By.XPATH,
									"//div[@class='text-body-medium break-words']").text
	print(description)
	sleep(4)
driver.close()


# COMMAND ----------

from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

# Specify the path to the ChromeDriver binary on DBFS
chrome_driver_path = "/dbfs/FileStore/tables/chrome"

# Create ChromeOptions and add "headless" argument
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("headless")

# Set up Chrome WebDriver with the specified path and options
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

driver.get("https://www.linkedin.com/login")
sleep(6)

linkedin_username = "#Linkedin Mail I'd"
linkedin_password = "#Linkedin Password"

# Use By.XPATH to find and interact with elements
driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[1]/input").send_keys(linkedin_username)
driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[2]/input").send_keys(linkedin_password)
sleep(3)
driver.find_element(By.XPATH, "/html/body/div/main/div[2]/div[1]/form/div[3]/button").click()

profiles = [
    'https://www.linkedin.com/in/vinayak-rai-a9b231193/',
    'https://www.linkedin.com/in/dishajindgar/',
    'https://www.linkedin.com/in/ishita-rai-28jgj/'
]

for profile_url in profiles:
    driver.get(profile_url)
    sleep(5)
    title = driver.find_element(By.XPATH, "//h1[@class='text-heading-xlarge inline t-24 v-align-middle break-words']").text
    print(title)
    description = driver.find_element(By.XPATH, "//div[@class='text-body-medium break-words']").text
    print(description)
    sleep(4)
# Close the WebDriver when done
driver.quit()


# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

pip install bs4

# COMMAND ----------

