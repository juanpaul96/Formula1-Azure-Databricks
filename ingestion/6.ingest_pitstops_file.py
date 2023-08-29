# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pitstops.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.json("f"{raw_folder_path}/pit_stops.json")
display(pit_stops_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

final_pit_stops_df = pit_stops_df \
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")


# COMMAND ----------

final_pit_stops_df = add_ingestion_date(pit_stops_df)
display(final_pit_stops_df) 

# COMMAND ----------

final_pit_stops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))
