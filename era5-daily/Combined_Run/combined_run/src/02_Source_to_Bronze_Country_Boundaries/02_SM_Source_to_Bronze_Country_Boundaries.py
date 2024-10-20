# Databricks notebook source
# MAGIC %md
# MAGIC # Efficient Geospatial Data Processing and Storage with GeoPandas and PySpark
# MAGIC
# MAGIC This notebook demonstrates a robust method for handling and processing geospatial data derived from shapefiles using GeoPandas, followed by efficient storage in Delta tables via PySpark. The procedures detailed here include validating and transforming geographic data, enriching it with metadata, and utilizing batch processing techniques to manage large datasets effectively. Each section is thoroughly documented, ensuring clarity in understanding and implementing the steps required for scalable and efficient geospatial data management.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of Libraries for Geospatial Data Processing
# MAGIC
# MAGIC This section includes the pip installation commands for libraries essential for working with geospatial data and providing progress feedback during operations:
# MAGIC
# MAGIC - `geopandas`: This library extends the capabilities of `pandas` to allow spatial operations on geometric types. It is crucial for handling and analyzing geospatial data, enabling operations such as spatial joins, projections, and geometry manipulations, which are fundamental for geographical analyses.
# MAGIC - `tqdm`: A versatile tool that provides a fast, extensible progress bar for loops and processes. It is useful for monitoring the progress of data processing tasks, especially when dealing with large datasets or long-running operations.
# MAGIC
# MAGIC These installations are fundamental for the tasks outlined in this notebook, ensuring the necessary tools are available for geospatial data manipulation and user feedback on process execution.

# COMMAND ----------

# MAGIC %pip install geopandas 
# MAGIC %pip install tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Necessary Libraries and Modules for Geospatial Data Analysis and Processing
# MAGIC
# MAGIC This code chunk imports the essential libraries and modules required for handling geospatial data, timing operations, and integrating with Spark for distributed data processing:
# MAGIC
# MAGIC - `os`: Provides a set of functions to interact with the operating system, such as navigating the file system or managing file paths, essential for data file management.
# MAGIC - `time`: Used for accessing time-related functions like delays (sleep), which can be useful for timing or pausing script execution, particularly in batch processes or during debugging.
# MAGIC - `pandas` (imported as `pd`): A foundational library for data manipulation and analysis, offering data structures and operations for manipulating numerical tables and time series.
# MAGIC - `geopandas` (imported as `gpd`): Extends `pandas` making it possible to handle geospatial data more conveniently by adding support for geographic operations on geometries directly within dataframes.
# MAGIC - `tqdm.auto`: Automatically selects and displays a smart progress bar based on the environment (notebook, terminal), ideal for tracking lengthy data processing tasks.
# MAGIC - `SparkSession`: Initializes a Spark session, the entry point to programming Spark with the Dataset and DataFrame API, facilitating distributed data processing.
# MAGIC - `pyspark.sql.functions`: Import specific functions like `lit`, which generates columns with literal (constant) values, and `current_timestamp`, which returns the current timestamp as a column, useful for adding standardized timestamps to data records.
# MAGIC
# MAGIC These imports equip the notebook to perform advanced data operations, including geospatial data analysis, efficient data handling, and integration with PySpark for scalable and distributed data processing.

# COMMAND ----------

from utils import *


# COMMAND ----------

# Get the Databricks workspace URL from Spark configuration
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)

# Print the workspace URL to verify
print(workspace_url)

# COMMAND ----------



# Check the workspace URL and set the delta table name or prevent execution
if workspace_url == 'dbc-ad3d47af-affb.cloud.databricks.com':
    delta_table_name = '`era5-daily-data`.bronze_dev.esri_worldcountryboundaries_global_bronze_test'

    # Example usage
    process_shapefile_to_delta(
        spark,
        shapefile_path='/Volumes/pilot/vector_files/country_boundaries/countries.shp',
        delta_table_name=delta_table_name,
        batch_size=100,
        target_crs=4326  # Set this to the EPSG code of the target CRS ## this will transform the file 
    )
else:
    print(f"Script is not configured to run in this workspace: {workspace_url}. Exiting...")


