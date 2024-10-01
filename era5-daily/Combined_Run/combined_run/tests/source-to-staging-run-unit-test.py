# Databricks notebook source
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install xarray netCDF4 h5netcdf
# MAGIC %pip install geopandas
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import xarray as xr

# COMMAND ----------

import subprocess


# List of unit test commands
unit_test_commands = [
    "python -m unittest unit-test_test_files_in_date_range.py"
]

# Loop through each unit test command and execute it
for command in unit_test_commands:
    result = subprocess.run(command, shell=True)
    
    # If the exit code is not 0, raise an error to stop the notebook
    if result.returncode != 0:
        raise Exception(f"Unit test failed: {command}")

print("All unit tests passed successfully.")

