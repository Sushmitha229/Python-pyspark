# Databricks notebook source
data = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()


# COMMAND ----------

# Save as CSV with header
df.write.mode("overwrite").option("header", True).csv("/FileStore/tables/sample_csv")

# Save as JSON
df.write.mode("overwrite").json("/FileStore/tables/sample_json")

# Save as Parquet
df.write.mode("overwrite").parquet("/FileStore/tables/sample_parquet")


# COMMAND ----------

# Save as Delta
df.write.format("delta").mode("overwrite").save("/FileStore/tables/sample_delta")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")


# COMMAND ----------

# Read CSV
csv_df = spark.read.option("header", True).csv("/FileStore/tables/sample_csv")
csv_df.write.mode("overwrite").saveAsTable("csv_table")

# Read JSON
json_df = spark.read.json("/FileStore/tables/sample_json")
json_df.write.mode("overwrite").saveAsTable("json_table")

# Read Parquet
parquet_df = spark.read.parquet("/FileStore/tables/sample_parquet")
parquet_df.write.mode("overwrite").saveAsTable("parquet_table")

# Read Delta
delta_df = spark.read.format("delta").load("/FileStore/tables/sample_delta")
delta_df.write.mode("overwrite").saveAsTable("delta_table")


# COMMAND ----------

spark.sql("SHOW TABLES").show()


# COMMAND ----------

df = spark.read.option("header", True).csv("/FileStore/tables/sample_csv")


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("your_table_name")


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("csv_table")


# COMMAND ----------

spark.sql("SHOW TABLES").show()


# COMMAND ----------

df_from_table = spark.table("csv_table")
df_from_table.show()


# COMMAND ----------

