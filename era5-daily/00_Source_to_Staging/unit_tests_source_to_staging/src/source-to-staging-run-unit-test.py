# Databricks notebook source
# MAGIC %pip install xarray netCDF4 h5netcdf
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


# Run the unit test files
!python unit-test_test_files_in_date_range.py
!python unit-test_test_no_files_in_date_range.py
