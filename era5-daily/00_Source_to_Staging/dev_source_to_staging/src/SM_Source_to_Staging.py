# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Handling and Processing Climate Data with PySpark and NetCDF
# MAGIC
# MAGIC This notebook demonstrates the techniques and processes involved in handling, analyzing, and managing large-scale climate data using xarray and netCDF4. It includes detailed explanations of how to manage data files based on specific attributes such as date ranges, and efficiently process and move large datasets using distributed computing principles. Each section is thoroughly documented to ensure clarity and ease of understanding, facilitating the replication and adaptation of these methods for similar data-intensive tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of Required Libraries
# MAGIC
# MAGIC This section includes the installation commands for Python libraries that are essential for handling and processing various data formats and performing parallel computations. Each library serves a specific purpose:
# MAGIC
# MAGIC - `xarray`: Used for labeling, indexing, and synchronizing multidimensional arrays, especially useful for working with climate data formats like netCDF.
# MAGIC - `netCDF4` and `h5netcdf`: These libraries provide interfaces to netCDF and HDF5 files, respectively, allowing for efficient storage and access to large datasets.
# MAGIC - `rioxarray`: Extends `xarray` to include tools for spatial analysis, such as rasterio integration for geospatial operations.

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION
# MAGIC %pip install xarray netCDF4 h5netcdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Necessary Libraries
# MAGIC
# MAGIC This code chunk imports the necessary libraries and modules required for data processing and analysis in a PySpark and scientific data context. Each import serves a specific function in the workflow:
# MAGIC
# MAGIC - `os`: Provides a way of using operating system dependent functionality like reading or writing to the filesystem.
# MAGIC - `xarray` (imported as `xr`): Facilitates working with labeled multi-dimensional arrays and datasets, especially useful for manipulating large climate data files like netCDF.
# MAGIC - `datetime`: Used to handle and manipulate date and time data, crucial for time-series analysis.
# MAGIC - `shutil`: Offers high-level file operations such as copying and archiving.
# MAGIC - `pandas` (imported as `pd`): Essential for data manipulation and analysis, particularly useful for handling tabular data with heterogeneously-typed columns.
# MAGIC - `netCDF4` (imported as `nc`): Enables interaction with netCDF files which are commonly used for storing scientific data.
# MAGIC - `lit`: A function from PySpark's SQL module that is used to add a new column with a constant value or to make explicit data type casting in DataFrame operations.
# MAGIC - `tqdm.auto`: Automatically selects an appropriate progress bar based on the environment (notebook, terminal, etc.), useful for monitoring the progress of data processing loops.

# COMMAND ----------


#import os
#import xarray as xr
#from datetime import datetime
#import shutil 
#import netCDF4 as nc

# COMMAND ----------

from utils import * 
import os
from pyspark.sql import SparkSession

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# COMMAND ----------




# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, run on a small subset of the data
    target_folder = '/Volumes/pilot/bronze_test/era5_daily_summary_dev'
    
    start_date = '1950-01-01'
    end_date = '1950-01-05'
    source_folder = '/Volumes/aer-processed/era5/daily_summary'
    prefix = 'reanalysis-era5-sfc-daily-'
    date_pattern = '%Y-%m-%d'
    source_file_attr = 'source_file'
    
    # Run your function with the small subset of data
    copy_and_move_files_by_date(start_date, 
                                end_date, 
                                source_folder, 
                                target_folder, 
                                prefix,
                                date_pattern,
                                source_file_attr)
    
    print("Function executed in the dev workspace on a small subset of the data.")
else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")
