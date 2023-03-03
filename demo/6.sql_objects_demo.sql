-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_py")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
SELECT *
  FROM race_results_python
  WHERE race_year = 2019;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_py;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap STRING,
race_time STRING,
point FLOAT,
position INT,
created_date TIMESTAMP
) 
USING parquet
LOCATION "/mnt/datalakelpbc/presentation/race_results_ext_sql"

-- COMMAND ----------

INSERT INTO race_results_ext_sql
SELECT * FROM demo.race_results_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM race_results_ext_sql;

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.p_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

SHOW TABLES;
