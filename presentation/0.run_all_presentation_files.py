# Databricks notebook source
v_results = dbutils.notebook.run("1.race_results",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("2.driver_standings",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("3.constructor_standings",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_results
