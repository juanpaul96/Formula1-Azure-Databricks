# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

constructors_schema = StructType(fields=[ 
StructField("constructorId", IntegerType(), False),
StructField("constructorRef", StringType(), True),
StructField("name", StringType(), True),
StructField("nationality", StringType(), True),
StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1datacoursedl/raw/constructors.json")
display(constructors_df) 

# COMMAND ----------

constructor_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
    .withColumn("ingestion_date",current_timestamp())
display(constructor_final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1datacoursedl/processed/constructors")
display(spark.read.parquet('/mnt/formula1datacoursedl/processed/constructors'))
