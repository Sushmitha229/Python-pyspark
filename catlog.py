# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS my_schema;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS my_schema.users (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   email STRING
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO my_schema.users VALUES
# MAGIC (1, 'Alice', 'alice@example.com'),
# MAGIC (2, 'Bob', 'bob@example.com');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_schema.users;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Delta table from existing table data
# MAGIC CREATE TABLE my_schema.users_delta
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM my_schema.users;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the format of the table
# MAGIC DESCRIBE EXTENDED my_schema.users_delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query Delta table
# MAGIC SELECT * FROM my_schema.users_delta;
# MAGIC
# MAGIC -- You can also use Delta features like DELETE or UPDATE
# MAGIC DELETE FROM my_schema.users_delta WHERE id = 2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN my_schema;
# MAGIC