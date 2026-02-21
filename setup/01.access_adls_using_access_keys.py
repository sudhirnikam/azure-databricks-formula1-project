# Databricks notebook source
formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dls7.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dls7.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dls7.dfs.core.windows.net/circuits.csv"))
