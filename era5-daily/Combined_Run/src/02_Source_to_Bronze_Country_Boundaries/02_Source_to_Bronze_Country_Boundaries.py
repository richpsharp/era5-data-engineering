# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Notebook Overview
# MAGIC
# MAGIC ### Objective:
# MAGIC This notebook is designed to process and transform country boundary shapefiles into bronze-tier Delta tables within the Databricks environment. It ensures that the geometries are properly transformed and stored, while handling execution logic based on the current workspace environment.
# MAGIC
# MAGIC ### Key Tasks:
# MAGIC 1. **Library Installations**: Installs necessary geospatial libraries such as `geopandas` and `tqdm` for shapefile processing.
# MAGIC 2. **Workspace Check**: Verifies the current Databricks workspace URL to ensure that the script only executes in the designated development environment.
# MAGIC 3. **Shapefile Processing**: 
# MAGIC    - Utilizes the `process_shapefile_to_delta()` function to load a country boundary shapefile, process its geometries, and write the results to a Delta table.
# MAGIC    - Includes a CRS transformation to EPSG 4326 for consistency in spatial operations.
# MAGIC 4. **Exit Condition**: If the script is not running in the development workspace, it safely exits without executing the processing function.
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC - **Shapefile Processing**: Conversion of vector data (country boundaries) into a Delta table format for downstream analysis.
# MAGIC - **CRS Transformation**: Ensures that all geometries are standardized using EPSG 4326 for spatial compatibility.
# MAGIC - **Workspace-Conditional Execution**: Prevents execution in non-development environments to ensure proper testing and data integrity.
# MAGIC

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

from utils import *


# COMMAND ----------

# Get the Databricks workspace URL from Spark configuration
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)

# Print the workspace URL to verify
print(workspace_url)

# COMMAND ----------

# MAGIC %md
# MAGIC **Dev workspace URL**

# COMMAND ----------

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC **Staging workspace URL**

# COMMAND ----------

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing Shapefile to Delta Table Based on Workspace URL
# MAGIC
# MAGIC This section of the notebook checks the current workspace URL to determine if the script should execute the shapefile processing function.
# MAGIC
# MAGIC - **Workspace Check**:  
# MAGIC   The script first checks if it is running in the development workspace (`'dbc-ad3d47af-affb.cloud.databricks.com'`). If the workspace matches, the script proceeds with processing the country boundaries shapefile into a Delta table.
# MAGIC   
# MAGIC - **Shapefile Processing**:
# MAGIC   - **`process_shapefile_to_delta()`**: This function reads the country boundary shapefile and loads it into a Delta table for further analysis.
# MAGIC   - **Parameters**:
# MAGIC     - **`spark`**: The current Spark session.
# MAGIC     - **`shapefile_path`**: The path to the shapefile, containing country boundary information.
# MAGIC     - **`delta_table_name`**: The name of the Delta table where the processed data will be stored.
# MAGIC     - **`batch_size`**: Number of records processed in each batch.
# MAGIC     - **`target_crs`**: EPSG code (`4326` in this case) used to transform the shapefile’s coordinates into the desired spatial reference system.
# MAGIC     
# MAGIC - **Exit Condition**:  
# MAGIC   If the script is not running in the designated workspace, it prints a message and exits without executing the function to avoid unnecessary processing in other environments.
# MAGIC

# COMMAND ----------



# Check the workspace URL and set the delta table name or prevent execution
if workspace_url == dev_workspace_url:
    delta_table_name = '`era5-daily-data`.bronze_dev.esri_worldcountryboundaries_global_bronze_test'

    # Example usage
    process_shapefile_to_delta(
        spark,
        shapefile_path='/Volumes/pilot/vector_files/country_boundaries/countries.shp',
        delta_table_name=delta_table_name,
        batch_size=100,
        target_crs=4326  # Set this to the EPSG code of the target CRS ## this will transform the file 
    )

elif workspace_url == staging_workspace_url:

    delta_table_name = '`era5-daily-data`.bronze_staging.esri_worldcountryboundaries_global_bronze'

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


