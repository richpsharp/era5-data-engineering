# Databricks notebook source
# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION # LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install xarray netCDF4 h5netcdf

# COMMAND ----------

import xarray as xr
from utils import *
from pyspark.sql import SparkSession

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# COMMAND ----------

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:

    # If in the dev workspace, run on a small subset of the data
    directory = '/Volumes/pilot/bronze_test/era5_daily_summary_dev'
    expected_lon = 1440
    expected_lat = 721

    # Run your function with the small subset of data 
    check_netcdf_files(directory = directory,
                       expected_lon=expected_lon, 
                       expected_lat=expected_lat)

    print("Function executed in the dev workspace on a small subset of the data.") 

else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")
