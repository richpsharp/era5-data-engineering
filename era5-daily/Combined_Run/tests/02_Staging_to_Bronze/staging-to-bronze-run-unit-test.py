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

# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION
# MAGIC %pip install xarray
# MAGIC %pip install netCDF4 h5netcdf
# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import subprocess

# List of unit test commands
unit_test_commands = [
    "python unit-test_netcdf_to_bronze_autoloader.py",
]

# Loop through each unit test command and execute it
for command in unit_test_commands:
    result = subprocess.run(command, shell=True)
    
    # If the exit code is not 0, raise an error to stop the notebook
    if result.returncode != 0:
        raise Exception(f"Unit test failed: {command}")

print("All unit tests passed successfully.")
