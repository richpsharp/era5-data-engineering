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
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  


from pyspark.sql.types import DateType
from pyspark.sql.functions import to_date
from datetime import datetime

import shutil
import xarray as xr
import netCDF4 as nc


# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# COMMAND ----------

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------



# Conditional logic to set the target_folder and execute the Delta table check based on the workspace URL
if workspace_url == dev_workspace_url:
    
    # Define the Delta table name in Databricks
    delta_table_name = "era5-daily-data.bronze_dev.era5_inventory_table"

    # Define the schema (removed 'date_created')
    table_schema = StructType([
        StructField("date_updated", DateType(), True),
        StructField("source_file", StringType(), True),
        StructField("Source_File_Path", StringType(), True),
        StructField("date_modified_in_s3", TimestampType(), True)
    ])

    # Check if the Delta table already exists
    if spark.catalog.tableExists(delta_table_name):
        print(f"Delta table exists: {delta_table_name}")
        
        # Load the Delta table
        delta_table = DeltaTable.forName(spark, delta_table_name)
        
        # Get the current schema of the Delta table
        current_schema = delta_table.toDF().schema
        
        # Compare the schemas (simple comparison for field names and types)
        if current_schema != table_schema:
            print("Schema differs, please manually adjust the schema.")
        else:
            print("Schema matches, no action needed.")
    else:
        print(f"Delta table does not exist: {delta_table_name}")
        
        # Create a new Delta table with the defined schema
        empty_df = spark.createDataFrame([], table_schema)
        empty_df.write.format("delta").saveAsTable(delta_table_name)
        print(f"Delta table created successfully: {delta_table_name}")
    
else:
    # Do not run if not in the dev workspace
    print("This function is not executed in this workspace.")



# COMMAND ----------

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, run on a small subset of the data
    target_folder = '/Volumes/era5-daily-data/bronze_dev/era5_gwsc_staging_folder'
    table_name="era5-daily-data.bronze_dev.era5_inventory_table"
    
    start_date = '1950-01-01'
    end_date = '1950-01-20'
    source_folder = '/Volumes/aer-processed/era5/daily_summary'
    prefix = 'reanalysis-era5-sfc-daily-'
    date_pattern = '%Y-%m-%d'
    source_file_attr = 'source_file'
    
    # Run your function with the small subset of data
    copy_and_move_files_by_date_and_keep_inventory(spark,
                                                   start_date, 
                                                   end_date, 
                                                   source_folder, 
                                                   target_folder, 
                                                    prefix,
                                                    table_name,
                                                    date_pattern,
                                                    source_file_attr)
    
    print("Function executed in the dev workspace on a small subset of the data.")
else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")
