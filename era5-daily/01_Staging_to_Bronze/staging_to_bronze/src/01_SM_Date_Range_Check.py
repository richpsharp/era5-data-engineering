# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sequence, explode, expr

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the catalog, schema, and table based on the workspace URL
if workspace_url == dev_workspace_url:
    # If in the dev workspace, set the catalog, schema, and table names
    catalog = 'pilot'
    schema = 'bronze_test'
    table = 'aer_era5_bronze_1950_to_present_test'
    
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
    
    # Check if there are any missing dates and display them or print a message
    if missing_dates.count() > 0:
        print("Missing dates:")
        missing_dates.show()
    else:
        print("There are no missing dates in this range.")
else:
    # Do not run the function if not in the dev workspace
    print("This check is not executed in this workspace.")

