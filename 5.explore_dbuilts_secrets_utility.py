# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utiliy
# MAGIC 1. Write and run dbutils.secret.help() to see all possible methods under dbutils
# MAGIC 2. Notice that when trying to expose the secret it will appear as 'REDACTED'

# COMMAND ----------

dbutils.secret.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key ='formuladl-account-key')
