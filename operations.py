# Databricks notebook source
rdd = sc.parallelize([1, 2, 3, 4])
mapped_rdd = rdd.map(lambda x: x * 2)
print(mapped_rdd.collect())  # Output: [2, 4, 6, 8]


# COMMAND ----------

rdd = sc.parallelize(["hello world", "spark rdd"])
flat_mapped_rdd = rdd.flatMap(lambda x: x.split(" "))
print(flat_mapped_rdd.collect())  # Output: ['hello', 'world', 'spark', 'rdd']


# COMMAND ----------

rdd = sc.parallelize([1, 2, 3, 4, 5])
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print(filtered_rdd.collect())  # Output: [2, 4]


# COMMAND ----------

rdd1 = sc.parallelize([('a', 1), ('b', 2)])
rdd2 = sc.parallelize([('a', 3), ('b', 4)])
joined_rdd = rdd1.join(rdd2)
print(joined_rdd.collect())  # Output: [('a', (1, 3)), ('b', (2, 4))]


# COMMAND ----------

rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 2)])
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
print(reduced_rdd.collect())  # Output: [('a', 3), ('b', 1)]


# COMMAND ----------

rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 2)])
grouped_rdd = rdd.groupByKey()
print([(k, list(v)) for k, v in grouped_rdd.collect()])
# Output: [('a', [1, 2]), ('b', [1])]


# COMMAND ----------

