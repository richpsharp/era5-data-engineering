# Databricks notebook source
# MAGIC %pip install xarray
# MAGIC %pip install netCDF4 h5netcdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries and Modules for Data Processing
# MAGIC
# MAGIC This section includes the import statements necessary for data manipulation, file management, and structured data handling within the notebook. Each library or module plays a critical role in the data processing workflow:
# MAGIC
# MAGIC - `pandas` (imported as `pd`): Essential for data manipulation and analysis, particularly useful for handling tabular data with heterogeneously-typed columns.
# MAGIC - `xarray` (imported as `xr`): Facilitates working with labeled multi-dimensional arrays and datasets, which is especially useful for manipulating large climate data files like netCDF.
# MAGIC - `os`: Provides a way of using operating system dependent functionality like reading or writing to the filesystem, crucial for managing data files and directories.
# MAGIC - `pyspark.sql.types`: This module includes classes that define the structure of DataFrames in PySpark, such as `StructType` and `StructField`, along with specific data types (`FloatType`, `StringType`, `TimestampType`, `LongType`, `BinaryType`). These are used to specify schema definitions for Spark DataFrames, ensuring data consistency and structure.
# MAGIC - `datetime`: Used to handle and manipulate date and time data, which is crucial for time-series analysis and operations based on time conditions.
# MAGIC
# MAGIC By importing these modules and libraries, the notebook is equipped to handle a variety of data operations, from basic file interactions to complex data transformations within distributed environments.
# MAGIC

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

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the target_folder based on the workspace URL
if workspace_url == dev_workspace_url:

    # If in the dev workspace, run on a small subset of the data
    source_file_location = '/Volumes/pilot/bronze_test/era5_daily_summary_dev'
    checkpoint_location = '/Volumes/pilot/bronze_test/checkpoints/source_to_bronze_era5'
    streaming_query_name = 'Load ERA5 Files'
    data_format = 'delta'
    table_name = 'aer_era5_bronze_1950_to_present_test'
    schema_name = 'bronze_test'
    catalog_name = 'pilot'
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
        date_created_attr=date_created_attr
    )

    print("Function executed in the dev workspace on a small subset of the data.") 

else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")

