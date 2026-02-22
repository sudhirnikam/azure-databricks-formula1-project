# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../common/configuration"

# COMMAND ----------

# MAGIC %run "../common/utils"

# COMMAND ----------

# DBTITLE 1,Setup OAuth Authentication
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-pincipal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-svc-principal-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dls7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dls7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dls7.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dls7.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dls7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Verify Authentication
# Verify access to raw container
display(dbutils.fs.ls("abfss://raw@formula1dls7.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

# DBTITLE 1,Cell 5
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

races_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))
races_with_timestamp_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'), col("file_date"))

races_selected_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

# DBTITLE 1,Cell 12
# races_selected_df.printSchema()
# races_selected_df.filter("race_timestamp like '2004-03-07'").show(10, False)
# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('abfss://processed@formula1dls7.dfs.core.windows.net/races')

# COMMAND ----------

races_selected_df.write \
    .mode("overwrite") \
    .partitionBy('race_year') \
    .format("delta") \
    .option("path", f"{processed_folder_path}/races") \
    .saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")