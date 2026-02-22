# Databricks notebook source
# %sql
# CREATE DATABASE demo;

# COMMAND ----------

# MAGIC %run "../etl/common/configuration"

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-pincipal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-secret')

spark.conf.set("fs.azure.account.auth.type.formula1dls7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dls7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dls7.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dls7.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dls7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results_python limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo.race_results_python;

# COMMAND ----------

# %sql
# CREATE STORAGE CREDENTIAL formula1_storage_cred
# WITH (AZURE_MANAGED_IDENTITY = '/subscriptions/e34013c4-12f3-4c55-ae75-ecbc50a53876/resourceGroups/databricks-course-rg/providers/Microsoft.Databricks/accessConnectors/databricks-access-connector');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION presentation_folder_path_v2;

# COMMAND ----------

race_results_df.write.format("delta").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_ext_py;

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_parquet_py").saveAsTable("demo.race_results_ext_parquet_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_ext_parquet_py;