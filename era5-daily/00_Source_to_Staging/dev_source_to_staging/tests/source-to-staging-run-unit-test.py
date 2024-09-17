# Databricks notebook source
# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install xarray netCDF4 h5netcdf
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


# Run the unit test files
!python unit-test_test_files_in_date_range.py
!python unit-test_test_no_files_in_date_range.py
