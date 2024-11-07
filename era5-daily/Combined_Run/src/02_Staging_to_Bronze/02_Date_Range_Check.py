# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for checking the completeness of the date range in the ERA5 bronze-tier dataset. It identifies whether there are any missing dates within the dataset and outputs the results.
# MAGIC
# MAGIC #### Key Components:
# MAGIC
# MAGIC - **Workspace URL Conditional Logic**:  
# MAGIC   The notebook first checks if it is running in the development workspace. If not, it exits without executing any further code.
# MAGIC
# MAGIC - **Catalog and Table Setup**:  
# MAGIC   If the notebook is running in the dev workspace, the catalog, schema, and table names are defined for the ERA5 climate data. The full table name (`aer_era5_bronze_1950_to_present_test`) is constructed for querying.
# MAGIC
# MAGIC - **Date Range Calculation**:
# MAGIC   - The notebook retrieves the earliest (`start_date`) and latest (`end_date`) dates from the ERA5 dataset.
# MAGIC   - It then generates a sequence of all dates between the `start_date` and `end_date` using an interval of 1 day.
# MAGIC
# MAGIC - **Missing Date Detection**:
# MAGIC   - The notebook performs a **left anti-join** between the generated sequence of full dates and the actual dates in the dataset to identify any missing dates.
# MAGIC   - If missing dates are found, they are displayed in the output. If no dates are missing, a message is printed confirming that all dates are present.
# MAGIC
# MAGIC - **Conditional Execution**:
# MAGIC   The notebook only runs in the development workspace. If the script is executed in a different environment, it exits without performing the date range check.
# MAGIC
# MAGIC This notebook ensures that the ERA5 dataset has complete coverage of dates, allowing the user to detect and address any gaps in the data before further processing.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sequence, explode, expr

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
    
    # Load the table
    df = SparkSession.builder.getOrCreate().table(full_table_name)
    
    # Get the earliest and latest dates
    date_range = df.selectExpr("min(time) as start_date", "max(time) as end_date").collect()[0]
    start_date = date_range['start_date']
    end_date = date_range['end_date']
    
    # Create a DataFrame with the full range of dates using INTERVAL 1 DAY
    full_dates = SparkSession.builder.getOrCreate().createDataFrame(
        [(start_date, end_date)],
        ["start_date", "end_date"]
    ).select(explode(sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))).alias("date"))

    # Join with the original DataFrame to find missing dates
    missing_dates = full_dates.join(df, full_dates["date"] == df["time"], "leftanti")
    
    # Check if there are any missing dates and display them or save them
    if missing_dates.count() > 0:
        print("Missing dates:")
        missing_dates.show()

        # Define the Delta table name for storing missing dates
        missing_dates_table = f"{catalog}.{schema}.missing_dates"
    
        # Save the missing dates as a Delta table
        missing_dates.write.format("delta").mode("overwrite").saveAsTable(missing_dates_table)
    else:
        print("There are no missing dates in this range.")

elif workspace_url == staging_workspace_url:

    # If in the staging workspace, set the catalog, schema, and table names
    catalog = '`era5-daily-data`'
    schema = 'bronze_staging'
    table = 'aer_era5_bronze_1950_to_present_staging_interpolation'
    
    # Construct the full table name
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Load the table
    df = SparkSession.builder.getOrCreate().table(full_table_name)
    
    # Get the earliest and latest dates
    date_range = df.selectExpr("min(time) as start_date", "max(time) as end_date").collect()[0]
    start_date = date_range['start_date']
    end_date = date_range['end_date']
    
    # Create a DataFrame with the full range of dates using INTERVAL 1 DAY
    full_dates = SparkSession.builder.getOrCreate().createDataFrame(
        [(start_date, end_date)],
        ["start_date", "end_date"]
    ).select(explode(sequence(col("start_date"), col("end_date"), expr("INTERVAL 1 DAY"))).alias("date"))

    # Join with the original DataFrame to find missing dates
    missing_dates = full_dates.join(df, full_dates["date"] == df["time"], "leftanti")
    
    # Check if there are any missing dates and display them or save them
    if missing_dates.count() > 0:
        print("Missing dates:")
        missing_dates.show()

        # Define the Delta table name for storing missing dates
        missing_dates_table = f"{catalog}.{schema}.missing_dates"
    
        # Save the missing dates as a Delta table
        missing_dates.write.format("delta").mode("overwrite").saveAsTable(missing_dates_table)
    else:
        print("There are no missing dates in this range.")


else:
    # Do not run the function if not in the dev or staging workspace
    print("This check is not executed in this workspace.")

