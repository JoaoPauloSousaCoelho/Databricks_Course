# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dp203datalakelpbc/processed

# COMMAND ----------

storage_account_name = 'dp203datalakelpbc'
storage_account_access_key = 'N0uP87kmZO2GHv14JSipvCLFNDhQlkIxr1wXk+7C80Kx/+Zo4dckz0Kr4kYjuAScJ3pqMSQP/v0f+AStiMaYUQ=='
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)


# COMMAND ----------

blob_container = 'raw'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/circuits.csv"
#salesDf = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

#races_df = spark.read.option('header', True).option('inferSchema', True).csv(filePath)
races_df = spark.read\
.option('inferSchema', True)\
.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

processed_folder_path
