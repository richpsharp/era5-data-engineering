# Databricks notebook source
from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"


# COMMAND ----------

if workspace_url != dev_workspace_url: 
    ## Skip the rest of the notebook
    dbutils.notebook.exit("Not in dev workspace. Skipping unit tests.")

else: 
    print("We are in dev workspace, proceed with the unit test")


# COMMAND ----------

# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install xarray netCDF4 h5netcdf 
# MAGIC
# MAGIC
# MAGIC

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

