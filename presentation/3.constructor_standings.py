# Databricks notebook source
# MAGIC %md 
# MAGIC #Dataframes for reporting
# MAGIC ####Create a dataframe that satisfies the data used in the page: 
# MAGIC https://www.bbc.com/sport/formula1/drivers-world-championship/standings Drivers tab.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

##pulling the data
races_results_df = spark.read.parquet(f"{processed_folder_path}/races_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

main_df = races_results_df\
.groupBy("race_year", "team")\
.agg(sum("points").alias("total_points"),
count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(main_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructor_standings_df = main_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

constructor_standings_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructor_standings")
