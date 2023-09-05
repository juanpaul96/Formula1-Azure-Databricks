# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.json(f"{raw_folder_path}/constructors.json")
display(constructors_df) 

# COMMAND ----------

constructor_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")
display(constructor_final_df) 

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Write output to parquet file

# COMMAND ----------

##constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
##display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

##OPTIONAL:
##Write data in the DB
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------


