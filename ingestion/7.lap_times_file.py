# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

lap_times_schema = StructType(fields=[ 
StructField("raceId", IntegerType(), True),
StructField("driverId", IntegerType(), True),
StructField("lap", IntegerType(), True),
StructField("position", IntegerType(), True),
StructField("time", StringType(), True),
StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1datacoursedl/raw/lap_times")
display(lap_times_df) 

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_lap_times_df = lap_times_df \
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumn("ingestion_date",current_timestamp())
display(final_lap_times_df) 

# COMMAND ----------

final_lap_times_df.write.mode("overwrite").parquet("/mnt/formula1datacoursedl/processed/lap_times")
display(spark.read.parquet("/mnt/formula1datacoursedl/processed/lap_times"))
