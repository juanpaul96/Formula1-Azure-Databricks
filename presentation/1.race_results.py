# Databricks notebook source
# MAGIC %md 
# MAGIC #Dataframes for reporting
# MAGIC ####Create a dataframe that satisfies the data used in the page: 
# MAGIC https://www.bbc.com/sport/formula1/2020/abu-dhabi-grand-prix/results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

##pulling the data
races_df = spark.read.parquet(f"{processed_folder_path}/races")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

races_df = races_df.withColumnRenamed("name","race_name")
races_df = races_df.withColumnRenamed("date","race_date")
circuits_df = circuits_df.withColumnRenamed("location", "circuit_location")
drivers_df = drivers_df.withColumnRenamed("number", "driver_number")
drivers_df = drivers_df.withColumnRenamed("name", "driver_name")
drivers_df = drivers_df.withColumnRenamed("nationality", "driver_nationality")
constructor_df = constructor_df.withColumnRenamed("name", "team")
results_df = results_df.withColumnRenamed("time", "race_time")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

main_df = results_df.join(races_df, results_df.race_id == races_df.race_id, "left") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left")\
    .join(constructor_df, results_df.constructor_id == constructor_df.constructor_id, "left")\
    .filter(races_df.race_year.isin(2020,2019))
display(main_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_df = main_df.join(circuits_df, main_df.circuit_id == circuits_df.circuit_id, "left")\
    .select(main_df.race_year, main_df.race_name, main_df.race_date, circuits_df.circuit_location, main_df.driver_name, main_df.driver_number, main_df.driver_nationality, main_df.team, main_df.grid, main_df.fastest_lap, main_df.race_time, main_df.points, main_df.position)\
    .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

##race_results_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races_results")

# COMMAND ----------

race_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
