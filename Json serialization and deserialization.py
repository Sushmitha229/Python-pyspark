# Databricks notebook source
import json

data = {
    "name": "Alice",
    "age": 30,
    "is_member": True,
    "hobbies": ["reading", "cycling"]
}

# Serialization: Python object to JSON string
json_string = json.dumps(data)
print(json_string)


# COMMAND ----------

json_data = '{"name": "Alice", "age": 30, "is_member": true, "hobbies": ["reading", "cycling"]}'

# Deserialization: JSON string to Python object
data = json.loads(json_data)
print(data)
print(data["name"])
