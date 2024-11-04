# Databricks notebook source
# Install necessary packages
%pip install boto3 s3fs aiobotocore tqdm

# COMMAND ----------

## restarting
dbutils.library.restartPython() 

# COMMAND ----------

### importing
from pyspark.sql import SparkSession
import os
import time
from datetime import datetime, timedelta
from utils import *

# COMMAND ----------


# Function to read the last execution time from a file
def get_last_execution_time(file_path):
    if os.path.exists(file_path):
        print(f"Loading last execution time from: {file_path}")  # Print the file path
        with open(file_path, 'r') as file:
            last_time_str = file.read().strip()
            print(f"Last execution time found in file: {last_time_str}")  # Print the timestamp read from the file
            return datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
    else:
        print(f"File not found: {file_path}")  # Print if the file doesn't exist
        return None

# Function to update the last execution time in the file
def update_last_execution_time(file_path):
    with open(file_path, 'w') as file:
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file.write(current_time_str)


# COMMAND ----------


# Path to the last_execution file
last_execution_file = '/Volumes/cmip6-daily-data/gwsc-cmip6-daily/nex-gddp-cmip6/download_file_tracking/dev_last_execution_time.txt'

# Get the last execution time
last_execution_time = get_last_execution_time(last_execution_file) 


# Print the result
if last_execution_time:
    print(f"Last execution time loaded: {last_execution_time}")
else:
    print("No last execution time found, or file does not exist.")

# COMMAND ----------

# Get the Databricks workspace URL from Spark configuration
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)

# Print the workspace URL to verify
print(workspace_url)

# COMMAND ----------


#### Detecting which workspace the code is in and parameterizing in accordance with the workspace ######

# Check if 24 hours have passed since the last execution
if last_execution_time is None or datetime.now() - last_execution_time >= timedelta(hours=24):
    # Detecting which workspace the code is in and parameterizing in accordance with the workspace
    if workspace_url == 'dbc-ad3d47af-affb.cloud.databricks.com':
        start_year = int(1950)
        end_year = int(2100)
        models = ['ACCESS-ESM1-5', 'BCC-CSM2-MR','CanESM5','CMCC-ESM2']
        variables = ['tas','pr','tasmin','tasmax']
        scenarios = ['historical', 'ssp126', 'ssp370', 'ssp585'] ## add ssp245
        output_folder = '/Volumes/cmip6-daily-data/gwsc-cmip6-daily/nex-gddp-cmip6'
        num_workers = 30  # dependent on the cores

        # Applying the function
        download_multiple_models_daily(
            start_year=start_year, 
            end_year=end_year, 
            models=models, 
            variables=variables, 
            scenarios=scenarios, 
            output_folder=output_folder, 
            num_workers=num_workers, 
            log_func=None
        )

        # Update the last execution time after the successful run
        update_last_execution_time(last_execution_file)
    else:
        print(f"Script is not configured to run in this workspace: {workspace_url}. Exiting...")
else:
    print("This script can only be run once in 24 hours. Exiting...")


