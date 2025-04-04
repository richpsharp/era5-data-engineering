# Databricks notebook source
# MAGIC %sh
# MAGIC which python

# COMMAND ----------

# MAGIC %python
# MAGIC import sys
# MAGIC print(sys.executable)
# MAGIC dbutils

# COMMAND ----------

# MAGIC %python
# MAGIC import io
# MAGIC import time
# MAGIC import os
# MAGIC from config import ERA5_SOURCE_VOLUME_PATH
# MAGIC
# MAGIC start = time.time()
# MAGIC source_path = f"{ERA5_SOURCE_VOLUME_PATH}/daily_summary/reanalysis-era5-sfc-daily-1950-01-01.nc"
# MAGIC target_path = f'/local_disk0/tmp_{os.path.basename(source_path)}'
# MAGIC dbutils.fs.cp('abfss://' + source_path, f'file:{target_path}')
# MAGIC with open(target_path, "rb") as f:
# MAGIC     file_binary = f.read()
# MAGIC
# MAGIC file_obj = io.BytesIO(file_binary)
# MAGIC print(f'took {time.time()-start:.2f}s to load')

# COMMAND ----------

# MAGIC %python
# MAGIC import time
# MAGIC
# MAGIC from utils.file_utils import copy_file_to_mem
# MAGIC from utils.file_utils import copy_mem_file_to_path
# MAGIC from utils.file_utils import hash_bytes
# MAGIC from utils.file_utils import is_netcdf_file_valid
# MAGIC
# MAGIC path = '/Volumes/aer-processed/era5/daily_summary/reanalysis-era5-sfc-daily-1950-12-12.nc'
# MAGIC file_in_mem = copy_file_to_mem(path)
# MAGIC start = time.time()
# MAGIC result = is_netcdf_file_valid(file_in_mem, path)
# MAGIC print(f'{result} in {time.time()-start:.2f}s')
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import datetime
# MAGIC from dateutil.relativedelta import relativedelta
# MAGIC import glob
# MAGIC import logging
# MAGIC import os
# MAGIC import re
# MAGIC import time
# MAGIC import collections
# MAGIC
# MAGIC import boto3
# MAGIC from urllib.parse import urlparse
# MAGIC from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
# MAGIC from config import ERA5_INVENTORY_TABLE_NAME
# MAGIC from config import ERA5_SOURCE_VOLUME_PATH
# MAGIC from config import ERA5_STAGING_VOLUME_ID
# MAGIC from config import AWS_SECRET_SCOPE
# MAGIC from config import AWS_SECRET_KEY_ID
# MAGIC from config import AWS_ACCESS_KEY_ID
# MAGIC from config import ERA5_SOURCE_VOLUME_FQDN
# MAGIC from databricks.sdk.runtime import spark
# MAGIC from pyspark.sql import Row
# MAGIC from tqdm import tqdm
# MAGIC from utils.catalog_support import get_catalog_schema_fqdn
# MAGIC from utils.catalog_support import create_schema_if_not_exists
# MAGIC from utils.file_utils import copy_file_to_mem
# MAGIC from utils.file_utils import copy_mem_file_to_path
# MAGIC from utils.file_utils import hash_bytes
# MAGIC from utils.file_utils import is_netcdf_file_valid
# MAGIC from utils.file_utils import copy_file_from_s3_to_mem
# MAGIC from utils.table_definition_loader import create_table
# MAGIC from utils.table_definition_loader import load_table_struct
# MAGIC from utils.catalog_support import get_unity_volume_location
# MAGIC
# MAGIC LOGGER = logging.getLogger(__name__)
# MAGIC LOGGER.setLevel(logging.DEBUG)
# MAGIC
# MAGIC # data starts here and we'll use it to set a threshold for when the data should
# MAGIC # be pulled
# MAGIC ERA5_START_DATE = datetime.datetime(1950, 1, 1).date()
# MAGIC DELTA_MONTHS = 3  # always search at least 3 months prior
# MAGIC
# MAGIC
# MAGIC def main():
# MAGIC     """Entrypoint."""
# MAGIC     global_start_time = time.time()
# MAGIC     schema_fqdn_path = get_catalog_schema_fqdn()
# MAGIC     create_schema_if_not_exists(schema_fqdn_path)
# MAGIC     target_volume_fqdn_path = f"{schema_fqdn_path}.{ERA5_STAGING_VOLUME_ID}"
# MAGIC     LOGGER.debug(f"create a volume at {target_volume_fqdn_path}")
# MAGIC     spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume_fqdn_path}")
# MAGIC     target_directory = os.path.join(
# MAGIC         "/Volumes", target_volume_fqdn_path.replace(".", "/")
# MAGIC     )
# MAGIC     LOGGER.debug(f"volume created, target directory is: {target_directory}")
# MAGIC
# MAGIC     table_definition = load_table_struct(
# MAGIC         ERA5_INVENTORY_TABLE_DEFINITION_PATH, ERA5_INVENTORY_TABLE_NAME
# MAGIC     )
# MAGIC     inventory_table_fqdn = f"{schema_fqdn_path}.{ERA5_INVENTORY_TABLE_NAME}"
# MAGIC     LOGGER.debug(f"creating {inventory_table_fqdn}")
# MAGIC     create_table(inventory_table_fqdn, table_definition)
# MAGIC     LOGGER.debug(f"created {inventory_table_fqdn} successfully")
# MAGIC     latest_date_query = f"""
# MAGIC         SELECT MAX(data_date) AS latest_date
# MAGIC         FROM {inventory_table_fqdn}
# MAGIC     """
# MAGIC     latest_date_df = spark.sql(latest_date_query)
# MAGIC     latest_date = latest_date_df.collect()[0]["latest_date"]
# MAGIC     LOGGER.debug(
# MAGIC         f"latest date seen in the query of {inventory_table_fqdn} is {latest_date}"
# MAGIC     )
# MAGIC     if latest_date is None:
# MAGIC         latest_date = ERA5_START_DATE
# MAGIC     LOGGER.debug(f"working date is {latest_date}")
# MAGIC
# MAGIC     start_date = latest_date - relativedelta(months=DELTA_MONTHS)
# MAGIC     end_date = datetime.date.today()
# MAGIC
# MAGIC     start_time = time.time()
# MAGIC     source_directory = os.path.join(ERA5_SOURCE_VOLUME_PATH, "daily_summary")
# MAGIC     LOGGER.debug(f"about to search {source_directory}")
# MAGIC
# MAGIC     # This is the hard-coded pattern for era5 daily
# MAGIC     pattern = re.compile(r"reanalysis-era5-sfc-daily-(\d{4}-\d{2}-\d{2})\.nc$")
# MAGIC     # sorting here in case the job doesn't complete we will have been working from the 
# MAGIC     # oldest date to the newest date, so querying the database won't kick us too far
# MAGIC     # forward in time if we haven't finished the past.
# MAGIC     # TODO: just have this do ALL the missing dates and also re-test 3 months prior
# MAGIC
# MAGIC     era5_source_s3_bucket_path = get_unity_volume_location(ERA5_SOURCE_VOLUME_FQDN)
# MAGIC     aws_access_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_ACCESS_KEY_ID)
# MAGIC     aws_secret_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_SECRET_KEY_ID)
# MAGIC
# MAGIC     boto3_session = boto3.Session(
# MAGIC         aws_access_key_id=aws_access_key,
# MAGIC         aws_secret_access_key=aws_secret_key
# MAGIC     )
# MAGIC     s3 = boto3_session.resource('s3')
# MAGIC     bucket_id = urlparse(era5_source_s3_bucket_path).netloc
# MAGIC     era5_file_prefix = urlparse(era5_source_s3_bucket_path).path.lstrip('/')
# MAGIC     LOGGER.debug(f'bucket id is : {bucket_id}')
# MAGIC     bucket = s3.Bucket(bucket_id)
# MAGIC    
# MAGIC     filtered_files = sorted([
# MAGIC         (file_date, s3_object.key)
# MAGIC         for s3_object in bucket.objects.filter(Prefix=era5_file_prefix)
# MAGIC         if s3_object.key.endswith(".nc")
# MAGIC         and (match := pattern.search(os.path.basename(s3_object.key)))
# MAGIC         and (file_date := datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date())
# MAGIC         and start_date <= file_date <= end_date
# MAGIC     ])
# MAGIC
# MAGIC main()
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import boto3
# MAGIC
# MAGIC from config import AWS_SECRET_SCOPE
# MAGIC from config import AWS_SECRET_KEY_ID
# MAGIC from config import AWS_ACCESS_KEY_ID
# MAGIC
# MAGIC current_principal = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# MAGIC print("Current principal:", current_principal)
# MAGIC
# MAGIC print(AWS_SECRET_SCOPE)
# MAGIC print(AWS_ACCESS_KEY_ID)
# MAGIC print(AWS_SECRET_KEY_ID)
# MAGIC aws_access_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_ACCESS_KEY_ID)
# MAGIC aws_secret_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_SECRET_KEY_ID)
# MAGIC s3 = boto3.client(
# MAGIC     's3',
# MAGIC     aws_access_key_id=aws_access_key,
# MAGIC     aws_secret_access_key=aws_secret_key,
# MAGIC )
# MAGIC
# MAGIC bucket_name = "gwsc-unity"
# MAGIC prefix = "b5102ff9-14c3-48c2-868c-edc61f1d2ff4/volumes/af9da9e2-2e7a-4301-a086-a294f53511a1"  # Optional: specify a prefix to filter objects
# MAGIC
# MAGIC # List objects in the bucket (filtered by prefix if provided)
# MAGIC response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
# MAGIC
# MAGIC if 'Contents' in response:
# MAGIC     for obj in response['Contents']:
# MAGIC         print(obj['Key'])
# MAGIC else:
# MAGIC     print("No objects found in the bucket or the prefix did not match any objects.")
# MAGIC
