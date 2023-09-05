-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_processed.constructors

-- COMMAND ----------

DROP TABLE  f1_presentation.calculated_race_results;
CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS 
SELECT r.race_year,
c.name AS team_name,
d.name AS driver_name,
rs.position,
rs.points,
11 - rs.position AS calculated_points
FROM results AS rs
JOIN drivers AS d ON (rs.driver_id = d.driver_id)
JOIN constructors AS c ON (rs.constructor_id = c.constructor_id)
JOIN races AS r ON (rs.race_id = r.race_id)
WHERE rs.position <= 10;

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------


