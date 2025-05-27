# Databricks notebook source
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/shared_uploads/priya240480@gmail.com/data-1.csv")
 
# Show the data
display(df)


# COMMAND ----------

df.printSchema();