# Databricks notebook source
response = requests.get("https://api.energy-charts.info/total_power")
json_data = response.json()

# Print the keys to understand the structure
print("Top-level keys:", json_data.keys())

# Optionally print the full JSON (or sample of it)
import json
print(json.dumps(json_data, indent=2)[:1500])



# COMMAND ----------

import requests
import json

url = "https://api.energy-charts.info/total_power"
response = requests.get(url)

print("Status Code:", response.status_code)

# Try printing the full response
try:
    json_data = response.json()
    print("Top-level keys:", json_data.keys())
    print("Sample content:\n", json.dumps(json_data, indent=2)[:2000])
except Exception as e:
    print("Error parsing JSON:", e)
    print("Raw response text:\n", response.text[:2000])



# COMMAND ----------

import requests

url = "https://api.energy-charts.info/total_power"  # or whatever URL you're using
response = requests.get(url)

# Check the status and raw content
print("Status Code:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))

# Print the raw response (first 1000 characters)
print("Raw response preview:")
print(response.text[:1000])






# COMMAND ----------

url = "https://api.energy-charts.info/power?country=DE&start=2023-01-01T00:00&end=2023-01-02T00:00"
response = requests.get(url)

# Confirm it's JSON before parsing
if "application/json" in response.headers.get("Content-Type", ""):
    json_data = response.json()
    print("Top-level keys:", json_data.keys())
else:
    print("Not a JSON response. Here's the raw content:\n", response.text[:1000])


# COMMAND ----------

# Databricks notebook source
# ---------------------------------------------
# STEP 1: Import required libraries
# ---------------------------------------------
import requests
import json

# Define API endpoint and parameters
country = "de"
start_date = "2023-01-01"
end_date = "2023-01-02"
api_url = f"https://api.energy-charts.info/public_power?country={country}&start={start_date}&end={end_date}"

# Call the API
response = requests.get(api_url)

# Check response status
if response.status_code != 200:
    print("API request failed with status code:", response.status_code)
    print("Response content:", response.text)
else:
    try:
        json_data = response.json()
        print("Top-level keys in the JSON response:", json_data.keys())
        print("Sample content:", json.dumps(json_data, indent=2)[:1000])
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
        print("Raw response content:", response.text[:1000])



# COMMAND ----------

# Assuming the data is under the 'data' key
if "data" in json_data:
    records = json_data["data"]


    # Proceed with converting records to a Spark DataFrame
    df = spark.createDataFrame(records)
    # Further processing...
else:
    print("The 'data' key is not present in the JSON response.")
    print("Available keys:", json_data.keys())


# COMMAND ----------

production_types = json_data["production_types"]

print(f"Type of production_types: {type(production_types)}")
print(f"Length of production_types: {len(production_types)}")
print("First element of production_types:")
print(production_types[0])







# COMMAND ----------

first_element = production_types[0]
if isinstance(first_element, dict):
    print("Keys of first element:", first_element.keys())
else:
    print("First element is not a dict, it's:", type(first_element))


# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col

# Extract unix_seconds and convert to datetime
unix_seconds = json_data["unix_seconds"]
timestamps = pd.to_datetime(unix_seconds, unit='s')

# Initialize dictionary with timestamp column
data_dict = {"timestamp": timestamps}

# Loop over production_types and add each production type data to dict
for prod in production_types:
    name = prod["name"]               # e.g. "coal"
    values = prod["data"]             # list of values
    data_dict[name] = values          # add as a column

# Check if all columns have the same length as timestamps
lengths = [len(v) for v in data_dict.values()]
print("Lengths of columns:", lengths)
assert all(length == lengths[0] for length in lengths), "Lengths mismatch!"

# Create pandas DataFrame
pdf = pd.DataFrame(data_dict)

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)

# Show result
display(df)


# COMMAND ----------

import requests
import json

url = "https://api.energy-charts.info/total_power"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"API request failed: {response.status_code}")

json_data = response.json()

# Print the top-level keys and first 1000 chars prettified
print("Top-level keys:", json_data.keys())
print(json.dumps(json_data, indent=2)[:1000])




# COMMAND ----------

import re

def clean_column_name(name):
    # Replace any character that is NOT a letter, number, or underscore with underscore
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)





# COMMAND ----------

data_dict = {"timestamp": timestamps}

for prod in production_types:
    original_name = prod["name"]
    clean_name = clean_column_name(original_name)
    values = prod["data"]
    data_dict[clean_name] = values


# COMMAND ----------

print("Columns:", list(data_dict.keys()))


# COMMAND ----------

import re
import pandas as pd
from pyspark.sql.functions import col

def clean_column_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

unix_seconds = json_data["unix_seconds"]
production_types = json_data["production_types"]

timestamps = pd.to_datetime(unix_seconds, unit='s')

data_dict = {"timestamp": timestamps}

for prod in production_types:
    original_name = prod["name"]
    clean_name = clean_column_name(original_name)
    values = prod["data"]
    data_dict[clean_name] = values

print("Columns after cleaning:", list(data_dict.keys()))

lengths = [len(v) for v in data_dict.values()]
assert all(length == lengths[0] for length in lengths), "Column length mismatch!"

pdf = pd.DataFrame(data_dict)

df = spark.createDataFrame(pdf)

df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

table_name = "energy_total_power_clean"

df.write.format("delta").mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

# Step 1: Import libraries
import requests
import json
import re
import pandas as pd
from pyspark.sql.functions import col

# Step 2: Define API URL and fetch data
url = "https://api.energy-charts.info/total_power"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"API request failed with status code: {response.status_code}")

json_data = response.json()

# Step 3: Define function to clean column names
def clean_column_name(name):
    # Replace any character not a-z, A-Z, 0-9 or _ with _
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

# Step 4: Extract data from JSON
unix_seconds = json_data["unix_seconds"]
production_types = json_data["production_types"]

# Convert unix timestamps to pandas datetime
timestamps = pd.to_datetime(unix_seconds, unit='s')

# Build data dictionary starting with timestamp column
data_dict = {"timestamp": timestamps}

# Add production type data with cleaned column names
for prod in production_types:
    original_name = prod["name"]
    clean_name = clean_column_name(original_name)
    values = prod["data"]
    data_dict[clean_name] = values

# Check lengths match
lengths = [len(v) for v in data_dict.values()]
assert all(length == lengths[0] for length in lengths), "Column length mismatch!"

# Step 5: Create pandas DataFrame then convert to Spark DataFrame
pdf = pd.DataFrame(data_dict)
df = spark.createDataFrame(pdf)

# Cast timestamp column to Spark timestamp type
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Step 6: Preview the data
display(df)

# Step 7: Save as Delta table in Databricks
table_name = "energy_total_power_clean"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Data successfully saved to Delta table `{table_name}`.")
