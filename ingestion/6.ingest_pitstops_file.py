# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pitstops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

pit_stops_schema = StructType(fields=[ 
StructField("raceId", IntegerType(), True),
StructField("driverId", IntegerType(), True),
StructField("stop", IntegerType(), True),
StructField("lap", IntegerType(), True),
StructField("time", StringType(), True),
StructField("duration", StringType(), True),
StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiline",True) \
.json("/mnt/formula1datacoursedl/raw/pit_stops.json")
display(pit_stops_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_pit_stops_df = pit_stops_df \
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumn("ingestion_date",current_timestamp())
display(final_pit_stops_df) 

# COMMAND ----------

final_pit_stops_df.write.mode("overwrite").parquet("/mnt/formula1datacoursedl/processed/pit_stops")
display(spark.read.parquet("/mnt/formula1datacoursedl/processed/pit_stops"))
