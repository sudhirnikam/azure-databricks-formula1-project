# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../common/configuration"

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())\
                                              .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet("abfss://processed@formula1dls7.dfs.core.windows.net/constructors")

# COMMAND ----------

constructor_final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", f"{processed_folder_path}/constructors") \
    .saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors limit 10;

# COMMAND ----------

constructor_final_df.show(10, False)

# COMMAND ----------

dbutils.notebook.exit("Success")