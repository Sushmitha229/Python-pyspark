# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession

# Fetch data from the API
url = "https://api.energy-charts.info/total_power"
response = requests.get(url)
data = response.json()


# COMMAND ----------

# Check top-level keys of the JSON response
print(data.keys())



# COMMAND ----------

# Flatten the JSON (assuming nested under 'data')
df_pd = pd.json_normalize(data['production_types'])

# Convert Pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Show data
df_spark.show(5)


# COMMAND ----------

df_spark.write.mode("overwrite").saveAsTable("total_power_data")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))




# COMMAND ----------

"/FileStore/tables/Crude_Oil_data.csv"





# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))




# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))




# COMMAND ----------

crude_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/Crude_Oil_data.csv")
crude_df.show(5)


# COMMAND ----------

crude_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/Crude_Oil_data.csv")
gasoline_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/RBOB_Gasoline_data.csv")


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, year, month, dayofmonth

for name, df in [("crude_df", crude_df), ("gasoline_df", gasoline_df)]:
    df = df.withColumn("Date", to_timestamp("Date", "yyyy-MM-dd")) \
           .withColumn("Year", year("Date")) \
           .withColumn("Month", month("Date")) \
           .withColumn("Day", dayofmonth("Date"))
    globals()[name] = df


# COMMAND ----------

crude_df.printSchema()



# COMMAND ----------

from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=["open", "close", "volume"],  # lowercase column names
    outputCols=["open", "close", "volume"]  # can also rename if needed
).setStrategy("median")  # or "mean" or "mode"

crude_df = imputer.fit(crude_df).transform(crude_df)
gasoline_df = imputer.fit(gasoline_df).transform(gasoline_df)


# COMMAND ----------

from pyspark.sql.functions import col, round


# COMMAND ----------

# For Crude Oil
crude_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/Crude_Oil_data.csv")
crude_df.show(5)



# COMMAND ----------

from pyspark.sql.functions import col, round

crude_df = crude_df.withColumn("Price_Range", col("high") - col("low")) \
                   .withColumn("Daily_Return", round((col("close") - col("open")) / col("open"), 4))

crude_df.select("Date", "open", "close", "high", "low", "Price_Range", "Daily_Return").show(5)


# COMMAND ----------

# Read the gasoline data CSV into a Spark DataFrame
gasoline_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/RBOB_Gasoline_data.csv")

# Check the first few rows
gasoline_df.show(5)


# COMMAND ----------

from pyspark.sql.functions import col, round

gasoline_df = gasoline_df.withColumn("Price_Range", col("high") - col("low")) \
                         .withColumn("Daily_Return", round((col("close") - col("open")) / col("open"), 4))


# COMMAND ----------

gasoline_df.select("Date", "open", "close", "high", "low", "Price_Range", "Daily_Return").show(5)


# COMMAND ----------

# Import required function
from pyspark.sql.functions import expr

# Get 75th percentile for 'volume' column
volume_threshold = crude_df.approxQuantile("volume", [0.75], 0.05)[0]

# Filter crude and gasoline data
crude_df_filtered = crude_df.filter(col("volume") > volume_threshold)
gasoline_df_filtered = gasoline_df.filter(col("volume") > volume_threshold)


# COMMAND ----------

from pyspark.sql.functions import to_date, dayofweek

# Ensure Date column is in date format
crude_df_filtered = crude_df_filtered.withColumn("Date", to_date(col("Date")))
gasoline_df_filtered = gasoline_df_filtered.withColumn("Date", to_date(col("Date")))

# Filter only Monday (2) to Friday (6) [1=Sunday, 7=Saturday]
crude_df_filtered = crude_df_filtered.filter((dayofweek(col("Date")) >= 2) & (dayofweek(col("Date")) <= 6))
gasoline_df_filtered = gasoline_df_filtered.filter((dayofweek(col("Date")) >= 2) & (dayofweek(col("Date")) <= 6))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import avg

# Define window spec
window_spec = Window.orderBy("Date").rowsBetween(-6, 0)  # 7-day window

# Add moving average to each DataFrame
crude_df_filtered = crude_df_filtered.withColumn("7D_MA_Close", round(avg("close").over(window_spec), 2))
gasoline_df_filtered = gasoline_df_filtered.withColumn("7D_MA_Close", round(avg("close").over(window_spec), 2))


# COMMAND ----------

# Add a Ticker column to each DataFrame
crude_df_filtered = crude_df_filtered.withColumn("Ticker", expr("'Crude'"))
gasoline_df_filtered = gasoline_df_filtered.withColumn("Ticker", expr("'Gasoline'"))

# Combine both
combined_df = crude_df_filtered.select("Date", "Ticker", "close").unionByName(
    gasoline_df_filtered.select("Date", "Ticker", "close")
)

# Pivot the table
pivot_df = combined_df.groupBy("Date").pivot("Ticker").agg(round(avg("close"), 2))
pivot_df.show()


# COMMAND ----------

# Save filtered DataFrames to Delta tables
crude_df_filtered.write.format("delta").mode("overwrite").saveAsTable("crude_filtered")
gasoline_df_filtered.write.format("delta").mode("overwrite").saveAsTable("gasoline_filtered")
pivot_df.write.format("delta").mode("overwrite").saveAsTable("fuel_close_pivot")


# COMMAND ----------

crude_check = spark.table("crude_filtered")
crude_check.show(5)

gasoline_check = spark.table("gasoline_filtered")
gasoline_check.show(5)

pivot_check = spark.table("fuel_close_pivot")
pivot_check.show(5)
