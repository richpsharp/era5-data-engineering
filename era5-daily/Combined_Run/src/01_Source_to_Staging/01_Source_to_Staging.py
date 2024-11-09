# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### **Notebook Overview**
# MAGIC This notebook is responsible for moving ERA5 climate data from the raw source to the staging area in Databricks. It includes conditional logic to process a small subset of data in the development environment and ensures the proper creation and validation of Delta tables. The notebook primarily focuses on handling data ingestion, table schema checks, and file processing for effective data management in the pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Outline**
# MAGIC
# MAGIC 1. **Introduction and Setup**
# MAGIC    - Install the required packages (e.g., `numpy`, `xarray`, `netCDF4`, `h5netcdf`).
# MAGIC    
# MAGIC 2. **Workspace URL Configuration**
# MAGIC    - Set up the workspace URL to determine whether the notebook is running in the development environment or another workspace.
# MAGIC
# MAGIC 3. **Conditional Logic for Delta Table Validation**
# MAGIC    - Checks if the Delta table exists and compares its schema.
# MAGIC    - Creates a new Delta table if it doesn't exist, and ensures that schema consistency is maintained.
# MAGIC
# MAGIC 4. **Data Processing and File Movement**
# MAGIC    - If the notebook is running in the development workspace, it processes a small subset of files for a defined date range.
# MAGIC    - Files are moved from the source folder to the staging folder, and the inventory of processed files is maintained in the Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of Required Libraries
# MAGIC
# MAGIC This Jupyter notebook cell installs the required Python packages for processing climate data in NetCDF format. Below is an explanation of each package and the reasoning behind the specific versions:
# MAGIC
# MAGIC 1. **`numpy==1.26.4`**:
# MAGIC    - The user specifies that version **1.26.4** of NumPy should be used to ensure compatibility with other libraries or to avoid issues with the latest versions.
# MAGIC    - NumPy is a fundamental package for numerical computations in Python.
# MAGIC
# MAGIC 2. **`xarray`**:
# MAGIC    - Xarray is a powerful library used for working with labeled multi-dimensional arrays, commonly applied for handling climate data in NetCDF format.
# MAGIC
# MAGIC 3. **`netCDF4`**:
# MAGIC    - The `netCDF4` package provides an interface for reading and writing NetCDF files, a format commonly used in scientific data for large, multi-dimensional datasets.
# MAGIC
# MAGIC 4. **`h5netcdf`**:
# MAGIC    - H5NetCDF is an alternative library for handling NetCDF4 files, specifically those using HDF5 as the underlying format. It provides similar functionality to `netCDF4`, with potential performance benefits in certain scenarios.
# MAGIC
# MAGIC ## Reasoning for Specific Versions:
# MAGIC - The notebook specifies **NumPy version 1.26.4** to ensure compatibility with other libraries or avoid issues that may arise with newer versions.
# MAGIC - The note to avoid the latest version of NumPy likely relates to compatibility issues with packages such as `xarray` and `netCDF4`.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ##
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
# MAGIC # Importing Utilities
# MAGIC
# MAGIC The line `from utils import *` imports all functions and variables from a module named `utils`. Here's what this implies:
# MAGIC
# MAGIC - **Purpose of `utils`**:  
# MAGIC   The `utils` module is likely a custom Python module that contains utility functions or helper methods that are commonly used throughout the notebook.
# MAGIC   
# MAGIC - **Wildcard Import (`*`)**:  
# MAGIC   The `*` operator imports everything from the `utils` module, including all functions, classes, and variables defined in it. However, this practice is typically avoided in larger projects as it can make it harder to track what exactly is being imported and could lead to name conflicts.
# MAGIC
# MAGIC
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
# MAGIC **Staging workspace URL**
# MAGIC

# COMMAND ----------

staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delta Table Existence and Schema Validation
# MAGIC
# MAGIC - This section of the code checks if the script is running in the development workspace (`dev_workspace_url`).
# MAGIC   
# MAGIC - If the script is in the development workspace:
# MAGIC   - It defines the Delta table name (`era5_inventory_table`) and the schema for the table, which includes columns like `date_updated`, `source_file`, and `date_modified_in_s3`.
# MAGIC   - The code checks if the Delta table already exists:
# MAGIC     - If the table exists, it compares its current schema with the predefined schema.
# MAGIC     - If the schemas match, no further action is needed. If they differ, a manual adjustment is recommended.
# MAGIC   - If the table does not exist, it creates a new Delta table with the specified schema.
# MAGIC
# MAGIC - If the script is not in the dev workspace, the function does not run.
# MAGIC

# COMMAND ----------



# Conditional logic to set the target_folder and execute the Delta table check based on the workspace URL
if workspace_url == dev_workspace_url:
    
    # Define the Delta table name in Databricks
    delta_table_name = "`era5-daily-data`.bronze_dev.era5_inventory_table"

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


elif workspace_url == staging_workspace_url:
    # Define the Delta table name in Databricks
    delta_table_name = "`era5-daily-data`.bronze_staging.era5_inventory_table"

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

# MAGIC %md
# MAGIC ### Conditional Execution and File Processing
# MAGIC
# MAGIC - **Workspace-Based Logic**: This code executes only if the current workspace is the development workspace. It processes a small subset of data for testing and validation purposes.
# MAGIC - **Key Variables**:
# MAGIC   - **`target_folder`**: The folder where the processed files will be moved.
# MAGIC   - **`table_name`**: The Delta table where the file inventory is maintained.
# MAGIC   - **`start_date` / `end_date`**: Defines the range of dates for which files will be processed.
# MAGIC   - **`source_folder`**: The folder containing the original files to be moved.
# MAGIC   - **`prefix`**: Prefix used to identify files in the source folder.
# MAGIC   - **`date_pattern`**: The format used for the date in the file names.
# MAGIC   - **`source_file_attr`**: Column in the Delta table representing the source file name.
# MAGIC   
# MAGIC - **Function Execution**: 
# MAGIC   - The `copy_and_move_files_by_date_and_keep_inventory` function is executed to copy and move files from the source to the target folder while updating the Delta table inventory with the processed file information.
# MAGIC   
# MAGIC - **Workspace Check**: If not in the development workspace, the function does not execute, and a message is printed.
# MAGIC

# COMMAND ----------

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, run on a small subset of the data
    target_folder = '/Volumes/era5-daily-data/bronze_dev/era5_gwsc_staging_folder'
    table_name="`era5-daily-data`.bronze_dev.era5_inventory_table"
    
    start_date = '1950-01-01'
    end_date = '1950-12-31'
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

elif workspace_url == staging_workspace_url:
    # If in the staging workspace, run on the entire data
    target_folder = '/Volumes/era5-daily-data/bronze_staging/era5_gwsc_staging_folder'
    table_name="`era5-daily-data`.bronze_staging.era5_inventory_table"
    
    start_date = '1968-06-08'
    end_date = '1984-12-31'
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
    
    print("Function executed in the staging workspace on the entire data.")


else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")
