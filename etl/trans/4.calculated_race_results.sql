-- Databricks notebook source
-- USE f1_processed;

-- COMMAND ----------

-- CREATE TABLE f1_presentation.calculated_race_results
-- USING parquet
-- AS
-- SELECT races.race_year,
--        constructors.name AS team_name,
--        drivers.name AS driver_name,
--        results.position,
--        results.points,
--        11 - results.position AS calculated_points
--   FROM results 
--   JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
--   JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
--   JOIN f1_processed.races ON (results.race_id = races.race_id)
--  WHERE results.position <= 10

-- COMMAND ----------

-- MAGIC %run "../common/configuration"

-- COMMAND ----------

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

-- MAGIC %python
-- MAGIC calculated_race_results_df = spark.sql("""SELECT races.race_year,
-- MAGIC        constructors.name AS team_name,
-- MAGIC        drivers.name AS driver_name,
-- MAGIC        results.position,
-- MAGIC        results.points,
-- MAGIC        11 - results.position AS calculated_points
-- MAGIC   FROM results 
-- MAGIC   JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
-- MAGIC   JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
-- MAGIC   JOIN f1_processed.races ON (results.race_id = races.race_id)
-- MAGIC  WHERE results.position <= 10""")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC calculated_race_results_df.write \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .format("parquet") \
-- MAGIC     .option("path", f"{presentation_folder_path}/calculated_race_results") \
-- MAGIC     .saveAsTable("f1_presentation.calculated_race_results")

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results limit 10;

-- COMMAND ----------

