# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault.
# MAGIC 2. Set spark Config with App/Client id, Directory/Tenant Id & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

##1. Get client_id, tenant_id, and client_secret from key vault.
client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-client-secret')

# COMMAND ----------

##2. Set spark Config with App/Client id, Directory/Tenant Id & Secret

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#3. Call file system utility mount to mount the storage
# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1datacoursedl.dfs.core.windows.net/",
  mount_point = "/mnt/formula1datacoursedl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1datacoursedl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1datacoursedl/demo/circuits.csv"))

# COMMAND ----------

#4. Explore other file system utilities related to mount (list all mounts, unmount)
#TIP: If you ever have a problem where you're writing to the mount and the data is not visible for you, that means you mounted to a different storage. Run this command and see where that is actually pointing at
display(dbutils.fs.mounts())

#To unmount
dbutils.fs.unmount('/mnt/formula1datacoursedl/demo')
