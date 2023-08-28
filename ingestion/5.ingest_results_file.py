# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[ 
StructField("resultId", IntegerType(), False),
StructField("raceId", IntegerType(), True),
StructField("driverId", IntegerType(), True),
StructField("constructorId", IntegerType(), True),
StructField("number", IntegerType(), True),
StructField("grid", IntegerType(), True),
StructField("position", IntegerType(), True),
StructField("positionText", StringType(), True),
StructField("positionOrder", IntegerType(), True),
StructField("points", FloatType(), True),
StructField("laps", IntegerType(), True),
StructField("time", StringType(), True),
StructField("milliseconds", IntegerType(), True),
StructField("fastestLap", IntegerType(), True),
StructField("rank", IntegerType(), True),
StructField("fastestLapTime", StringType(), True),
StructField("fastestLapSpeed", StringType(), True),
StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json("/mnt/formula1datacoursedl/raw/results.json")
display(results_df) 

# COMMAND ----------

results_dropped_df = results_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_columns_df = results_dropped_df \
    .withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumn("ingestion_date",current_timestamp())
display(results_with_columns_df) 

# COMMAND ----------

results_with_columns_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1datacoursedl/processed/results")
display(spark.read.parquet("/mnt/formula1datacoursedl/processed/results"))
