-- Databricks notebook source
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `raw-folder-path-location` TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `processed-folder-path-location` TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `presentation-folder-path-location` TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `demo-folder-path-location` TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT USAGE, CREATE ON CATALOG databricks_course_ws TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT USAGE, CREATE, SELECT, MODIFY ON SCHEMA databricks_course_ws.demo 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT USAGE, CREATE, SELECT, MODIFY ON SCHEMA databricks_course_ws.f1_demo 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT USAGE, CREATE, SELECT, MODIFY ON SCHEMA databricks_course_ws.f1_raw 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT USAGE, CREATE, SELECT, MODIFY ON SCHEMA databricks_course_ws.f1_processed 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT USAGE, CREATE, SELECT, MODIFY ON SCHEMA databricks_course_ws.f1_presentation 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `raw-folder-path-location` 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `processed-folder-path-location` 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `presentation-folder-path-location` 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `demo-folder-path-location` 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;

-- COMMAND ----------

GRANT ALL PRIVILEGES ON CATALOG databricks_course_ws 
TO `a4518fb2-6eea-4a2d-b960-dc931cf38981`;