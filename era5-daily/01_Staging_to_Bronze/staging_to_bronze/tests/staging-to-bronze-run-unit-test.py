# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION
# MAGIC %pip install xarray
# MAGIC %pip install netCDF4 h5netcdf
# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Run the unit tests
!python unit-test_netcdf_to_bronze_autoloader.py


