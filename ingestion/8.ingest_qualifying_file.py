# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON files using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

qualifying_schema = StructType(fields=[ 
StructField("qualifyId", IntegerType(), True),
StructField("raceId", IntegerType(), True),
StructField("driverId", IntegerType(), True),
StructField("constructorId", IntegerType(), True),
StructField("number", IntegerType(), True),
StructField("position", IntegerType(), True),
StructField("q1", StringType(), True),
StructField("q2", StringType(), True),
StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline",True) \
.json(f"{raw_folder_path}/qualifying")
display(qualifying_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

final_qualifying_df = qualifying_df \
    .withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("constructorId","constructor_id")

# COMMAND ----------

final_qualifying_df = add_ingestion_date(qualifying_df)
display(final_qualifying_df) 

# COMMAND ----------

##final_qualifying_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
##display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

##OPTIONAL:
##Write data in the DB
final_qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
