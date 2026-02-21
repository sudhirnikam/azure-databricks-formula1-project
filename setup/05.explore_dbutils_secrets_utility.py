# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dls7.dfs.core.windows.net",
    formula1dl_account_key)

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dls7.dfs.core.windows.net/circuits.csv"))
