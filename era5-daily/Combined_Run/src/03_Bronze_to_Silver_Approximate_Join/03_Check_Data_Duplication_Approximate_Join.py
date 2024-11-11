# Databricks notebook source
#######
from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com" 

staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

if workspace_url == dev_workspace_url:
  ## Skip the rest of the notebook
  dbutils.notebook.exit("Skipping this code.")
elif workspace_url == staging_workspace_url:
  ## Skip the rest of the notebook
  dbutils.notebook.exit("Skipping this code.")
else: 
  print("Not skipping this code")

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for identifying and handling duplicate entries in the ERA5 climate dataset within the bronze-tier table. It focuses on spatial and temporal consistency by checking for duplicate `latitude`, `longitude`, and `time` combinations and verifying the integrity of key climate variables.
# MAGIC
# MAGIC ## Key Components:
# MAGIC
# MAGIC ### Workspace URL Conditional Logic
# MAGIC The notebook first checks the current workspace URL to determine if it is running in the development workspace (`dev_workspace_url`). This ensures that data processing and validation occur only in a controlled environment. If the script is executed outside the dev workspace, it will exit without performing any operations.
# MAGIC
# MAGIC ### Catalog and Table Setup
# MAGIC When running in the dev workspace, the notebook sets up the necessary catalog, schema, and table names. The full table name, `aer_era5_silver_approximate_1950_to_present_dev_interpolation`, is constructed for querying the bronze-tier ERA5 climate data. This table contains climate records such as temperature and precipitation for various time steps and geolocations.
# MAGIC
# MAGIC ### Data Quality Checks
# MAGIC The notebook performs crucial data quality checks, focusing on:
# MAGIC - **Duplicate Detection**: Identifies duplicate entries based on `latitude`, `longitude`, `grid_index` and `time` combinations.
# MAGIC - **Variable Integrity Checks**: Verifies the consistency of key climate variables (`mean_t2m_c`, `max_t2m_c`, `min_t2m_c`, `sum_tp_mm`) across duplicate entries to ensure data reliability.
# MAGIC
# MAGIC ### Query Execution and Output
# MAGIC The notebook executes queries to:
# MAGIC - Identify duplicate `latitude`, `longitude`, `grid_index` and `time` entries.
# MAGIC - Detect duplicates for each climate variable within the same spatial and temporal context.
# MAGIC - Save the results of these checks into Delta tables for further analysis and traceability.
# MAGIC
# MAGIC ### Conditional Execution
# MAGIC The notebook's logic is designed to execute the data quality checks only when run in the development workspace. This ensures that no data validation or duplication handling occurs inadvertently in production or staging environments, maintaining data integrity and operational safety.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This notebook ensures that key climate data variables in the bronze-tier table are accurately validated for duplicates. By checking and documenting any inconsistencies, it supports the reliability of climate data analyses and downstream processes.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com" 

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# Conditional logic to set the catalog, schema, and table based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, set the catalog, schema, and table names
    catalog = '`era5-daily-data`'
    schema = 'silver_dev'
    table = 'aer_era5_silver_approximate_1950_to_present_dev_interpolation'
    
    # Construct the full table name
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Load the table
    df = SparkSession.builder.getOrCreate().table(full_table_name)

    
    ### Identify duplicates based on latitude, longitude and grid index combinations within the same time 
    df_grid_index_lat_lon_duplicates = df.groupBy('grid_index', 'latitude', 'longitude', 'time') \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) 

    ### 
    # Join back with the original DataFrame to get all columns
    df_grid_index_lat_lon_duplicates = df_grid_index_lat_lon_duplicates.join(df, on=['grid_index', 'latitude', 'longitude', 'time'], how='inner') \
        .select("time", "latitude", "longitude", "count", "file_modified_in_s3", "source_file", 
                "Source_File_Path", "Ingest_Timestamp", "data_provider", "date_created","grid_index","country")

    # Save this to a Delta table
    df_grid_index_lat_lon_duplicates.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.approximate_join_time_lat_lon_grid_index_duplicates_with_details")



    # Identify the duplicates for each of the variables within the same time step
    variables = ["mean_t2m_c", "max_t2m_c", "min_t2m_c", "sum_tp_mm"]

    for var in variables:
        df_variable_duplicates = df.groupBy('latitude', 'longitude', 'time', var) \
            .agg(count("*").alias("count")) \
            .filter(col("count") > 1)

        # Join back with the original DataFrame to get all columns
        df_variable_duplicates = df_variable_duplicates.join(df, on=['latitude', 'longitude', 'time', var], how='inner') \
            .select("time", "latitude", "longitude", var, "count", "file_modified_in_s3", 
                    "source_file", "Source_File_Path", "Ingest_Timestamp", 
                    "data_provider", "date_created","grid_index","country")
        
        # Save each variable's duplicates to a separate Delta table
        df_variable_duplicates.write.format("delta").mode("overwrite") \
            .saveAsTable(f"{catalog}.{schema}.approximate_join_duplicates_{var}") 

elif workspace_url == staging_workspace_url:

    # If in the staging workspace, set the catalog, schema, and table names
    catalog = '`era5-daily-data`'
    schema = 'silver_staging'
    table = 'aer_era5_silver_approximate_1950_to_present_staging_interpolation_res5'
    
    # Construct the full table name
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Load the table
    df = SparkSession.builder.getOrCreate().table(full_table_name)

    
    ### Identify duplicates based on latitude, longitude and grid index combinations within the same time 
    df_grid_index_lat_lon_duplicates = df.groupBy('grid_index', 'latitude', 'longitude', 'time') \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) 

    ### 
    # Join back with the original DataFrame to get all columns
    df_grid_index_lat_lon_duplicates = df_grid_index_lat_lon_duplicates.join(df, on=['grid_index', 'latitude', 'longitude', 'time'], how='inner') \
        .select("time", "latitude", "longitude", "count", "file_modified_in_s3", "source_file", 
                "Source_File_Path", "Ingest_Timestamp", "data_provider", "date_created","grid_index","country")

    # Save this to a Delta table
    df_grid_index_lat_lon_duplicates.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.approximate_join_time_lat_lon_grid_index_duplicates_with_details")



    # Identify the duplicates for each of the variables within the same time step
    variables = ["mean_t2m_c", "max_t2m_c", "min_t2m_c", "sum_tp_mm"]

    for var in variables:
        df_variable_duplicates = df.groupBy('latitude', 'longitude', 'time', var) \
            .agg(count("*").alias("count")) \
            .filter(col("count") > 1)

        # Join back with the original DataFrame to get all columns
        df_variable_duplicates = df_variable_duplicates.join(df, on=['latitude', 'longitude', 'time', var], how='inner') \
            .select("time", "latitude", "longitude", var, "count", "file_modified_in_s3", 
                    "source_file", "Source_File_Path", "Ingest_Timestamp", 
                    "data_provider", "date_created","grid_index","country")
        
        # Save each variable's duplicates to a separate Delta table
        df_variable_duplicates.write.format("delta").mode("overwrite") \
            .saveAsTable(f"{catalog}.{schema}.approximate_join_duplicates_{var}") 

else:
    # Do not run the function if not in the dev or staging workspace
    print("This function is not executed in this workspace.")



