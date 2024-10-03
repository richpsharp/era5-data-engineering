# Databricks notebook source
from pyspark.sql import SparkSession

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the catalog, schema, and table based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, set the catalog, schema, and table names
    catalog = 'era5-daily-data'
    schema = 'bronze_dev'
    table = 'aer_era5_bronze_1950_to_present_test'
    
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
else:
    # Do not run the function if not in the dev workspace
    print("Data quality checks not executed in this workspace.")

