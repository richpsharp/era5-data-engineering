# Databricks notebook source
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
# MAGIC ### Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for performing data value checks on key climate variables in the bronze-tier Delta table, ensuring that the data is within expected ranges and identifying any outliers.
# MAGIC
# MAGIC #### Key Components:
# MAGIC
# MAGIC - **Workspace URL Conditional Logic**:  
# MAGIC   The notebook first checks whether it is running in the development workspace. If not, it exits without executing any further code.
# MAGIC
# MAGIC - **Catalog and Table Setup**:  
# MAGIC   If the notebook is running in the dev workspace, the catalog, schema, and table names are defined for the ERA5 climate data. The full table name (`aer_era5_bronze_1950_to_present_test`) is constructed for querying.
# MAGIC
# MAGIC - **Data Quality Checks**:
# MAGIC   The notebook performs value checks on four key variables, with specific thresholds for each:
# MAGIC   - **`mean_t2m_c`**: Checks that values are between `-123.15` and `100`.
# MAGIC   - **`max_t2m_c`**: Checks that values are between `-123.15` and `100`.
# MAGIC   - **`min_t2m_c`**: Checks that values are between `-123.15` and `100`.
# MAGIC   - **`sum_tp_mm`**: Checks that values are between `-1` and `100,000`.
# MAGIC
# MAGIC - **Query Execution and Output**:  
# MAGIC   For each variable, the notebook constructs and runs a SQL query to identify outliers. Any rows that fall outside the specified range are written to new Delta tables (e.g., `mean_t2m_c_check`, `max_t2m_c_check`, etc.), allowing for detailed review of data anomalies.
# MAGIC
# MAGIC - **Conditional Execution**:  
# MAGIC   The notebook only runs the data value checks in the dev workspace. If the script is executed in a different environment, it will exit without running the queries or saving any results.
# MAGIC
# MAGIC This notebook ensures that key climate data variables in the bronze-tier table are within expected bounds, providing data quality validation before further processing.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

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
    schema = 'bronze_dev'
    table = 'aer_era5_bronze_1950_to_present_dev_interpolation'
    
    # Construct the full table name
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Columns to include in all output Delta tables
    common_columns = """
    time,
    latitude,
    longitude,
    file_modified_in_s3,
    source_file,
    Source_File_Path,
    Ingest_Timestamp,
    data_provider,
    date_created
    """

    # Check for mean_t2m_c
    query_mean_t2m_c = f"""
    SELECT {common_columns}, mean_t2m_c
    FROM {full_table_name}
    WHERE mean_t2m_c < -123.15 OR mean_t2m_c > 100
    """
    df_mean_t2m_c = SparkSession.builder.getOrCreate().sql(query_mean_t2m_c)
    df_mean_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.mean_t2m_c_check")

    # Check for max_t2m_c
    query_max_t2m_c = f"""
    SELECT {common_columns}, max_t2m_c
    FROM {full_table_name}
    WHERE max_t2m_c < -123.15 OR max_t2m_c > 100
    """
    df_max_t2m_c = SparkSession.builder.getOrCreate().sql(query_max_t2m_c)
    df_max_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.max_t2m_c_check")

    # Check for min_t2m_c
    query_min_t2m_c = f"""
    SELECT {common_columns}, min_t2m_c
    FROM {full_table_name}
    WHERE min_t2m_c < -123.15 OR min_t2m_c > 100
    """
    df_min_t2m_c = SparkSession.builder.getOrCreate().sql(query_min_t2m_c)
    df_min_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.min_t2m_c_check")

    # Check for sum_tp_mm
    query_sum_tp_mm = f"""
    SELECT {common_columns}, sum_tp_mm
    FROM {full_table_name}
    WHERE sum_tp_mm < -1 OR sum_tp_mm > 100000
    """
    df_sum_tp_mm = SparkSession.builder.getOrCreate().sql(query_sum_tp_mm)
    df_sum_tp_mm.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.sum_tp_mm_check")
    
    # Print statement indicating successful execution
    print("Data quality checks executed and results saved in the dev workspace.")

elif workspace_url == staging_workspace_url:

    # If in the staging workspace, set the catalog, schema, and table names
    catalog = '`era5-daily-data`'
    schema = 'bronze_staging'
    table = 'aer_era5_bronze_1950_to_present_staging_interpolation'
    
    # Construct the full table name
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Columns to include in all output Delta tables
    common_columns = """
    time,
    latitude,
    longitude,
    file_modified_in_s3,
    source_file,
    Source_File_Path,
    Ingest_Timestamp,
    data_provider,
    date_created
    """

    # Check for mean_t2m_c
    query_mean_t2m_c = f"""
    SELECT {common_columns}, mean_t2m_c
    FROM {full_table_name}
    WHERE mean_t2m_c < -123.15 OR mean_t2m_c > 100
    """
    df_mean_t2m_c = SparkSession.builder.getOrCreate().sql(query_mean_t2m_c)
    df_mean_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.mean_t2m_c_check")

    # Check for max_t2m_c
    query_max_t2m_c = f"""
    SELECT {common_columns}, max_t2m_c
    FROM {full_table_name}
    WHERE max_t2m_c < -123.15 OR max_t2m_c > 100
    """
    df_max_t2m_c = SparkSession.builder.getOrCreate().sql(query_max_t2m_c)
    df_max_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.max_t2m_c_check")

    # Check for min_t2m_c
    query_min_t2m_c = f"""
    SELECT {common_columns}, min_t2m_c
    FROM {full_table_name}
    WHERE min_t2m_c < -123.15 OR min_t2m_c > 100
    """
    df_min_t2m_c = SparkSession.builder.getOrCreate().sql(query_min_t2m_c)
    df_min_t2m_c.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.min_t2m_c_check")

    # Check for sum_tp_mm
    query_sum_tp_mm = f"""
    SELECT {common_columns}, sum_tp_mm
    FROM {full_table_name}
    WHERE sum_tp_mm < -1 OR sum_tp_mm > 100000
    """
    df_sum_tp_mm = SparkSession.builder.getOrCreate().sql(query_sum_tp_mm)
    df_sum_tp_mm.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.sum_tp_mm_check")
    
    # Print statement indicating successful execution
    print("Data quality checks executed and results saved in the staging workspace.")


else:
    # Do not run the function if not in the dev or staging workspace
    print("Data quality checks not executed in this workspace.")

