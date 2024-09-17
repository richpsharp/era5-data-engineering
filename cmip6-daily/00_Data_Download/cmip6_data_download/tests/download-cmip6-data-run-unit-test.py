# Databricks notebook source
# Install necessary packages
%pip install boto3 s3fs aiobotocore tqdm

# COMMAND ----------


dbutils.library.restartPython()

# COMMAND ----------

# Run the unit test files
!python unit-test_download_file.py
!python unit-test_download_cmip6_netcdf_daily.py
!python unit-test_download_multiple_models_daily.py
!python unit-test_download_multiple_scenarios_daily.py

# COMMAND ----------

import subprocess

# List of unit test commands
unit_test_commands = [
    "python unit-test_download_file.py",
    "python unit-test_download_cmip6_netcdf_daily.py",
    "python unit-test_download_multiple_scenarios_daily.py",
    "python unit-test_download_multiple_models_daily.py"
]

# Loop through each unit test command and execute it
for command in unit_test_commands:
    result = subprocess.run(command, shell=True)
    
    # If the exit code is not 0, raise an error to stop the notebook
    if result.returncode != 0:
        raise Exception(f"Unit test failed: {command}")

print("All unit tests passed successfully.")

