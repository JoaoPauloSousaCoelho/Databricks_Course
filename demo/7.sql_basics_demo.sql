-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * 
  FROM f1_processed.drivers
  LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * 
  FROM f1_processed.drivers
  WHERE nationality = 'British'
  AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS data_of_birth
  FROM f1_processed.drivers
  WHERE nationality = 'British'
  AND dob >= '1990-01-01'
  ORDER BY dob DESC;

-- COMMAND ----------

SELECT * 
  FROM f1_processed.drivers
  ORDER BY nationality ASC, 
  dob DESC;
