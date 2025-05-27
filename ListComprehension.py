# Databricks notebook source
filenames = [
    "data1.csv",
    "report.txt",
    "summary.csv",
    "image.png",
    "sales_data.csv",
    "notes.docx"
]


# COMMAND ----------

csv_files = [file for file in filenames if file.endswith(".csv")]


# COMMAND ----------

print("CSV Files:", csv_files)
