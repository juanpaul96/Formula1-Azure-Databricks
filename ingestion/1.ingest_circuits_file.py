# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.cvs file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CVS file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula1datacoursedl/raw

# COMMAND ----------

#Header: it's telling to the df that the first row are the headers
#InferSchema: infers the types of data of the df -Use only in very small data and test env
circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/formula1datacoursedl/raw/circuits.csv")

# COMMAND ----------

##Raw data
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Structuring the data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[ 
StructField("circuitId", IntegerType(), False),
StructField("circuitRef", StringType(), True),
StructField("name", StringType(), True),
StructField("location", StringType(), True),
StructField("country", StringType(), True),
StructField("lat", DoubleType(), True),
StructField("lang", DoubleType(), True),
StructField("alt", IntegerType(), True),
StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema).csv("/mnt/formula1datacoursedl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Select the required Columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lang","alt")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Renaming Columns

# COMMAND ----------

circuits_rename_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lang","longitude") \
.withColumnRenamed("alt","altitude")
display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Adding the new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_rename_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6. Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/formula1datacoursedl/processed/circuits")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1datacoursedl/processed/circuits'))
