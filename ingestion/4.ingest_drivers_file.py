# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
StructField("forename", StringType(), True),
StructField("surname", StringType(), True),
])

# COMMAND ----------

drivers_schema = StructType(fields=[ 
StructField("driverid", IntegerType(), False),
StructField("driverRef", StringType(), True),
StructField("number", IntegerType(), True),
StructField("code", StringType(), True),
StructField("name", name_schema),
StructField("dob", StringType(), True),
StructField("nationality", StringType(), True),
StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/formula1datacoursedl/raw/drivers.json")
display(drivers_df) 

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

# COMMAND ----------

drivers_with_columns_df = drivers_dropped_df \
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname")))
display(drivers_with_columns_df) 

# COMMAND ----------

drivers_with_columns_df.write.mode("overwrite").parquet("/mnt/formula1datacoursedl/processed/drivers")
display(spark.read.parquet("/mnt/formula1datacoursedl/processed/drivers"))
