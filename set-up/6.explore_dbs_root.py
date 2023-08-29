# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders om DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

##Directories created by default in DBFS root.
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('sbfs:/FileStore/circuits.csv'))
