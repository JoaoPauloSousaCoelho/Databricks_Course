# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1_ingest_circuit_files", 0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("2_ingest_race_files", 0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("3_ingest_constructors_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("4_ingest_drivers_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("5_ingest_results_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("6_ingest_pit_stops_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("7_ingest_lap_times_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("8_ingest_qualifying_file", 0, {"p_data_source": "Ergast API"})
