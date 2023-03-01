# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read\
.option('inferSchema', True)\
.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")\
.withColumnRenamed("name", "race_name")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# left outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "outer")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Joins

# COMMAND ----------

race_circuits_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)
