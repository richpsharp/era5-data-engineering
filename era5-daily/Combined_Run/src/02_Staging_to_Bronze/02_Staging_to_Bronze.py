# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for processing data from the staging area and moving it to the bronze-tier Delta table. The process involves continuous streaming ingestion of NetCDF files, using Databricks' autoloader, to efficiently load and append data to the bronze table.
# MAGIC
# MAGIC #### Key Components:
# MAGIC - **Workspace URL Conditional Logic**:  
# MAGIC   The notebook checks if it is running in the development workspace before executing any data processing steps. If it is not in the dev environment, the script exits without executing.
# MAGIC
# MAGIC - **Data Ingestion**:  
# MAGIC   The notebook processes a subset of NetCDF files stored in the staging area. It uses the `netcdf_to_bronze_autoloader()` function to load the files and write them to a Delta table in the bronze tier.
# MAGIC
# MAGIC - **Streaming Query**:  
# MAGIC   The ingestion process uses a streaming query to continuously load new data and append it to the Delta table. Checkpointing is used to track progress and ensure fault tolerance.
# MAGIC
# MAGIC - **Schema Definition**:  
# MAGIC   The schema for the bronze table is defined to ensure that the loaded data is structured correctly, with fields for date updated, source file, and additional metadata.
# MAGIC
# MAGIC This notebook ensures efficient movement of data from staging to bronze, with logic in place to avoid redundant execution outside the development workspace.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Required Libraries
# MAGIC
# MAGIC This section installs the necessary Python libraries for handling NetCDF files and multidimensional arrays. The specific version of `numpy` (1.26.4) is required to avoid compatibility issues, and the latest version should not be used as it may cause the notebook to crash.
# MAGIC
# MAGIC - **`numpy`**: Used for handling large, multidimensional arrays and matrices.
# MAGIC - **`xarray`**: Provides a high-level interface for working with labeled multi-dimensional arrays, especially useful for NetCDF files.
# MAGIC - **`netCDF4` and `h5netcdf`**: Libraries for working with NetCDF files, a common format for storing climate data.
# MAGIC
# MAGIC Make sure these specific versions are installed to ensure the notebook runs without errors.
# MAGIC

# COMMAND ----------

# MAGIC %pip install numpy==1.26.4 #### please use this version of numpy ## DO NOT USE THE LATEST VERSION
# MAGIC %pip install xarray netCDF4 h5netcdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from utils import *
from pyspark.sql import SparkSession


# COMMAND ----------

# Define the expected netCDF data schema, so we can construct the Spark dataframe

## decide what your output table might look like

output_schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("mean_t2m_c", FloatType(), True),
    StructField("max_t2m_c", FloatType(), True),
    StructField("min_t2m_c", FloatType(), True),
    StructField("sum_tp_mm", FloatType(), True),
    StructField("file_modified_in_s3", TimestampType(), True),
    StructField("source_file", StringType(), True),
    StructField("Source_File_Path", StringType(), True),
    StructField("Ingest_Timestamp", TimestampType(), True),
    StructField("data_provider", StringType(), True),
    StructField("date_created", TimestampType(), True)
])

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Data Processing Based on Workspace
# MAGIC
# MAGIC This section contains logic that checks the workspace URL to determine if the script is running in the development environment (`dev_workspace_url`). If it is, the script processes a small subset of data and moves it from the staging area to the bronze Delta table using Databricks' autoloader. If not, the script exits without executing.
# MAGIC
# MAGIC - **Conditional Execution**:  
# MAGIC   The script runs only if it detects the workspace URL matches the development environment.
# MAGIC
# MAGIC - **Data Processing in Dev Workspace**:  
# MAGIC   - **Source File Location**: The location of the source NetCDF files to be processed.
# MAGIC   - **Checkpoint Location**: The directory where checkpoint data for the streaming process is stored.
# MAGIC   - **Streaming Query**: A name assigned to the streaming query used for loading files.
# MAGIC   - **Data Format**: The format of the data being processed (`delta`).
# MAGIC   - **Delta Table Name**: The table where the processed data is written.
# MAGIC   - **Write Mode**: Data is appended to the existing table.
# MAGIC   - **Data Provider**: The provider of the data, in this case, `Atmospheric and Environmental Research (AER)`.
# MAGIC
# MAGIC If the script is not running in the dev workspace, it exits and prints a message indicating no further action will be taken.
# MAGIC

# COMMAND ----------

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:

    # If in the dev workspace, run on a small subset of the data
    source_file_location = '/Volumes/era5-daily-data/bronze_dev/era5_gwsc_staging_folder'
    checkpoint_location = '/Volumes/era5-daily-data/bronze_dev/checkpoints/source_to_bronze_era5'
    reference_ds_path = '/Volumes/cmip6-daily-data/gwsc-cmip6-daily/nex-gddp-cmip6/ACCESS-ESM1-5/ssp245/pr/pr_day_ACCESS-ESM1-5_ssp245_r1i1p1f1_gn_2040_v1.1.nc'
    streaming_query_name = 'Load ERA5 Files'
    data_format = 'delta'
    table_name = 'aer_era5_bronze_1950_to_present_dev_interpolation'
    schema_name = 'bronze_dev'
    catalog_name = '`era5-daily-data`'
    write_mode = 'append'
    data_provider = 'Atmospheric and Environmental Research (AER)'
    date_created_attr = 'date_updated'

    # Run your function with the small subset of data 
    netcdf_to_bronze_autoloader(
        spark,
        source_file_location=source_file_location,
        output_schema=output_schema,
        checkpoint_location=checkpoint_location,
        streaming_query_name=streaming_query_name,
        data_format=data_format,
        table_name=table_name,
        schema_name=schema_name,
        catalog_name=catalog_name,
        write_mode=write_mode,
        data_provider=data_provider,
        date_created_attr=date_created_attr,
        reference_ds_path=reference_ds_path
    )

    print("Function executed in the dev workspace on a small subset of the data.") 

else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")

