# Databricks notebook source
delta%python
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-pincipal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-secret')

spark.conf.set("fs.azure.account.auth.type.formula1dls7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dls7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dls7.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dls7.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dls7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC MANAGED LOCATION "abfss://processed@formula1dls7.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC MANAGED LOCATION "abfss://presentation@formula1dls7.dfs.core.windows.net"