# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for the project
# MAGIC

# COMMAND ----------

def mount_adls(container_name,storage_account_name):
    #Get secrets from Key Vault.
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-service-principal-client-secret')

    #Set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    #Checks if the mount point already exists, if so, deletes it to create a new one
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        debutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    #Set mount
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    #Display mounts if all config is succesfull
    display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Containers
# MAGIC

# COMMAND ----------

mount_adls('raw','formula1datacoursedl')
mount_adls('presentation','formula1datacoursedl')
mount_adls('processed','formula1datacoursedl')
