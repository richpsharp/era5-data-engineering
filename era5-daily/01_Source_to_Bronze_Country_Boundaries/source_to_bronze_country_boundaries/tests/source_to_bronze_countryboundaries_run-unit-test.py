# Databricks notebook source
# MAGIC %pip install pytest pytest-mock
# MAGIC %pip install geopandas 
# MAGIC %pip install tqdm
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


# Run the unit test files
!python unit-test_process_shapefile_to_delta.py
