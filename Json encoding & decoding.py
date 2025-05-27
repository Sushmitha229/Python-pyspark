# Databricks notebook source
import json

# Python dictionary
data = {
    "name": "Alice",
    "age": 30,
    "is_student": False,
    "courses": ["Math", "Physics"],
    "address": {"city": "New York", "zip": "10001"}
}

# Encoding Python dictionary into JSON string
json_string = json.dumps(data)

print(json_string)


# COMMAND ----------

import json

# JSON string
json_string = '{"name": "Alice", "age": 30, "is_student": false, "courses": ["Math", "Physics"], "address": {"city": "New York", "zip": "10001"}}'

# Decoding JSON string into Python dictionary
data = json.loads(json_string)

print(data)
print(type(data))  # <class 'dict'>
