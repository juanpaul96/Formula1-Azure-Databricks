# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest racces.cvs file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CVS file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula1datacoursedl/raw

# COMMAND ----------

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/formula1datacoursedl/raw/races.csv")
display(circuits_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[ 
StructField("raceId", IntegerType(), False),
StructField("year", IntegerType(), True),
StructField("round", IntegerType(), True),
StructField("circuitId", IntegerType(), True),
StructField("name", StringType(), True),
StructField("date", DateType(), True),
StructField("time", StringType(), True),
StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv("/mnt/formula1datacoursedl/raw/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Select the required Columns

# COMMAND ----------

races_selected_df = races_df.select("raceId","year","round","circuitId","name","date","time")

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Renaming Columns

# COMMAND ----------

races_rename_df = races_selected_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id")
display(races_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Adding the new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

races_final_df = races_rename_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Write data to datalake as parquet, partitioning by race_year

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1datacoursedl/processed/races")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1datacoursedl/processed/races'))

# COMMAND ----------


