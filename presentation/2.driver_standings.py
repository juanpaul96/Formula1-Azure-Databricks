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
.groupBy("race_year", "driver_name", "driver_nationality", "team")\
.agg(sum("points").alias("total_points"),
count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(main_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings_df = main_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

##driver_standings_df.write.mode("overwrite").parquet(f"{processed_folder_path}/driver_standings")

# COMMAND ----------

driver_standings_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
