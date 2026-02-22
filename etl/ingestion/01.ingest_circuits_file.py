# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../common/configuration"

# COMMAND ----------

# MAGIC %run "../common/utils"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# DBTITLE 1,Setup OAuth Authentication
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-pincipal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-secret')

# COMMAND ----------

# DBTITLE 1,Configure Spark OAuth
spark.conf.set("fs.azure.account.auth.type.formula1dls7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dls7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dls7.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dls7.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dls7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Cell 5
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)



# COMMAND ----------

# DBTITLE 1,Cell 11
# circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

circuits_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("path", f"{processed_folder_path}/circuits") \
    .saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended f1_processed.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

# DBTITLE 1,Cell 12
display(spark.read.delta(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")