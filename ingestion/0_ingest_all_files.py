# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1_ingest_circuit_files", 0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
