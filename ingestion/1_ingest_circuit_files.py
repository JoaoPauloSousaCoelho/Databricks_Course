# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuit csv files

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1: Read the csv file using the sparker dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
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

circuits_df = spark.read\
                .option('header', True)\
                .schema(circuit_schema)\
                .csv(f"{raw_folder_path}/circuits.csv")                    

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step_2: Select the desired columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df= circuits_df.select(col("circuitid"), col("circuitRef"), col("name"),
                                        col("location"), col("country"),col("lat"), col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename Columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitid", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4- Add Ingestion Date to Dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_timestamp_date(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 5 - Write to datalake as a parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

#%r
#require(SparkR)

#df <- read.df( '/mnt/lpbcdatalake/raw/circuits.csv', "csv", header = "true", inferSchema = "true", na.strings = "NA")


# COMMAND ----------


