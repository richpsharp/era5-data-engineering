# Databricks notebook source
from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------

if workspace_url != dev_workspace_url: 
    ## Skip the rest of the notebook
    dbutils.notebook.exit("Not in dev workspace. Skipping unit tests.")

else: 
    print("We are in dev workspace, proceed with the unit test")

# COMMAND ----------

# MAGIC %pip install pytest pytest-mock
# MAGIC %pip install geopandas 
# MAGIC %pip install tqdm
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import subprocess

# List of unit test commands
unit_test_commands = [
    "python unit-test_process_shapefile_to_delta.py",
]

# Loop through each unit test command and execute it
for command in unit_test_commands:
    result = subprocess.run(command, shell=True)
    
    # If the exit code is not 0, raise an error to stop the notebook
    if result.returncode != 0:
        raise Exception(f"Unit test failed: {command}")

print("All unit tests passed successfully.")
