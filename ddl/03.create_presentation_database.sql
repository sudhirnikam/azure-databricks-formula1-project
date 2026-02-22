-- Databricks notebook source
-- MAGIC %python
-- MAGIC client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-id')
-- MAGIC tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-pincipal-tenant-id')
-- MAGIC client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-secret')
-- MAGIC
-- MAGIC spark.conf.set("fs.azure.account.auth.type.formula1dls7.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.formula1dls7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.formula1dls7.dfs.core.windows.net", client_id)
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dls7.dfs.core.windows.net", client_secret)
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dls7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "abfss://presentation@formula1dls7.dfs.core.windows.net"

-- COMMAND ----------

DESC DATABASE f1_presentation;

-- COMMAND ----------

SHOW TABLES IN f1_presentation;