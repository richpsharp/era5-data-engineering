# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Overview: 
# MAGIC
# MAGIC ### Purpose:
# MAGIC This notebook ensures the spatial quality of NetCDF files used in the ERA5 climate data pipeline. It verifies that the files in the development workspace have the correct spatial dimensions, specifically the expected number of longitude and latitude points, before further processing. 
# MAGIC
# MAGIC __Author:__ Sambadi Majumder | __Maintained:__ Sambadi Majumder |__Last Modified:__ 12/12/2024
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Components:
# MAGIC 1. **Package Installation**: Installs necessary packages such as `numpy`, `xarray`, and `netCDF4` for working with NetCDF files.
# MAGIC 2. **Utilities Import**: Imports utility functions required for processing and checking the NetCDF files.
# MAGIC 3. **Workspace Conditional Logic**: Executes the code only in the development workspace to ensure proper handling of smaller data subsets.
# MAGIC 4. **Spatial Dimension Check**:
# MAGIC     - Defines the expected number of longitude and latitude points (1440 and 721 respectively).
# MAGIC     - Uses the function `check_netcdf_files()` to verify if the files in the specified directory meet the expected dimensions.
# MAGIC     
# MAGIC ### Conditional Execution:
# MAGIC - The notebook checks the workspace environment and only runs the spatial dimension validation in the development workspace (`dev_workspace_url`). If not in the development workspace, the function is skipped to prevent unintended execution.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC This code chunk installs the required Python packages with specific versions for running the notebook. The specified version of `numpy` (1.26.4) is crucial to avoid compatibility issues that may cause the notebook to crash. Other required libraries, such as `xarray`, `netCDF4`, and `h5netcdf`, are installed for handling and processing NetCDF files.
# MAGIC
# MAGIC - **`numpy==1.26.4`**: Specifies the required version of numpy, avoiding the latest version to ensure stability.
# MAGIC - **`xarray`**: A library for working with labeled multi-dimensional arrays and datasets, useful for working with NetCDF data.
# MAGIC - **`netCDF4`** and **`h5netcdf`**: Libraries for reading and writing NetCDF files, which are a standard format for climate data.

# COMMAND ----------

# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION # LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install xarray netCDF4 h5netcdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Required Modules
# MAGIC
# MAGIC This section imports utility functions and initializes a Spark session to manage data processing and interactions with the Databricks environment.
# MAGIC
# MAGIC - **`from utils import *`**: Imports utility functions required for file handling, schema management, or other custom operations.
# MAGIC - **`from pyspark.sql import SparkSession`**: Initializes the Spark session, which is necessary for creating data frames and interacting with Databricks.
# MAGIC
# MAGIC These imports ensure that the notebook has access to all the necessary modules and functions for the following tasks.
# MAGIC

# COMMAND ----------


from utils import *
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC **This retrieves the current Databricks workspace URL to determine the execution environment.**

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# COMMAND ----------

# MAGIC %md
# MAGIC **The dev workspace URL**

# COMMAND ----------

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC **The staging workspace URL**

# COMMAND ----------

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Logic and Spatial Dimension Check
# MAGIC
# MAGIC This section sets up conditional logic to ensure the function is only executed in the development workspace. It checks NetCDF files in a specific directory to verify that they have the correct spatial dimensions.
# MAGIC
# MAGIC - **Conditional Logic**: 
# MAGIC   - Checks if the notebook is running in the development workspace (`dev_workspace_url`). If true, it proceeds with the execution; otherwise, the function is not executed.
# MAGIC   
# MAGIC - **Setting Variables**:
# MAGIC   - **`directory`**: Path to the directory where the NetCDF files are stored.
# MAGIC   - **`expected_lon`**: Expected number of longitude points in the NetCDF files (1440 points).
# MAGIC   - **`expected_lat`**: Expected number of latitude points in the NetCDF files (721 points).
# MAGIC   
# MAGIC - **Function Execution**: 
# MAGIC   - **`check_netcdf_files()`**: This function checks if the NetCDF files in the specified directory have the expected longitude and latitude dimensions.
# MAGIC   
# MAGIC - **Output**:
# MAGIC   - If executed in the development workspace, it prints that the function has been executed on a small subset of data. If not, it prints a message indicating that the function is not executed in the current workspace.
# MAGIC

# COMMAND ----------

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:

    # If in the dev workspace, run on a small subset of the data
    directory = '/Volumes/era5-daily-data/bronze_dev/era5_gwsc_staging_folder'
    expected_lon = 1440
    expected_lat = 721

    # Run your function with the small subset of data 
    check_netcdf_files(directory = directory,
                       expected_lon=expected_lon, 
                       expected_lat=expected_lat)

    print("Function executed in the dev workspace on a small subset of the data.") 

elif workspace_url == staging_workspace_url:

    # If in the dev workspace, run on a small subset of the data
    directory = '/Volumes/era5-daily-data/bronze_staging/era5_gwsc_staging_folder'
    expected_lon = 1440
    expected_lat = 721

    # Run your function with the small subset of data 
    check_netcdf_files(directory = directory,
                       expected_lon=expected_lon, 
                       expected_lat=expected_lat)

    print("Function executed in the staging workspace on the whole data.") 



else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")
