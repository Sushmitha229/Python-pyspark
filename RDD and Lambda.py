# Databricks notebook source
# Just use sc directly, do NOT create a new SparkContext
rdd = sc.parallelize([1, 2, 3, 4])



# COMMAND ----------

from pyspark import SparkContext

try:
    sc = SparkContext.getOrCreate()
except Exception as e:
    print(e)

# Now use sc
rdd = sc.parallelize([1, 2, 3])



# COMMAND ----------

# Don't create a new SparkContext in Databricks!

# Just use the existing SparkContext `sc`
# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Transformation: Multiply each element by 2
rdd2 = rdd.map(lambda x: x * 2)

# Action: Collect results
result = rdd2.collect()
print(result)  # Output: [2, 4, 6, 8, 10]

# Don't stop sc in Databricks — it’s managed by the platform



# COMMAND ----------

rdd2 = rdd.map(lambda x: x + 10)  # Add 10 to each element


# COMMAND ----------

filtered_rdd = rdd.filter(lambda x: x % 2 == 0)  # Keep even numbers only


# COMMAND ----------

sum_all = rdd.reduce(lambda a, b: a + b)  # Sum of all elements


# COMMAND ----------

# Assuming rdd is already defined, e.g.:
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Use reduce to sum all elements
sum_all = rdd.reduce(lambda a, b: a + b)

# Print the result
print(sum_all)  # Output: 15
