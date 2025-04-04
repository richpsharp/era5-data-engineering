"""ERA5 source to staging pipeline."""

import io
import datetime
from dateutil.relativedelta import relativedelta
import glob
import logging
import os
import re
import time
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed

from urllib.parse import urlparse
from config import AER_VOLUME_ROOT_PATH
from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
from config import ERA5_INVENTORY_TABLE_NAME
from config import ERA5_SOURCE_VOLUME_PATH
from config import ERA5_STAGING_VOLUME_ID
from config import AWS_SECRET_SCOPE
from config import AWS_SECRET_KEY_ID
from config import AWS_ACCESS_KEY_ID
from config import ERA5_SOURCE_VOLUME_FQDN
from databricks.sdk.runtime import spark
from pyspark.sql import Row
from tqdm import tqdm
from utils.catalog_support import get_catalog_schema_fqdn
from utils.catalog_support import create_schema_if_not_exists
from utils.file_utils import copy_file_to_mem
from utils.file_utils import copy_mem_file_to_path
from utils.file_utils import hash_bytes
from utils.file_utils import is_netcdf_file_valid
from utils.file_utils import copy_file_from_s3_to_mem
from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct
from utils.catalog_support import get_unity_volume_location

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# data starts here and we'll use it to set a threshold for when the data should
# be pulled
ERA5_START_DATE = datetime.datetime(1950, 1, 1).date()
DELTA_MONTHS = 3  # always search at least 3 months prior


def process_file(file_date, source_file_path, target_directory, existing_hashes, counts_dict, new_entries):
    """Process a single file: download from S3, validate, hash, and re-upload.

    This function performs the following steps:
      1. Downloads the file from S3.
      2. Validates the NetCDF file using xarray.
      3. Computes its SHA-256 hash.
      4. Skips processing if the hash already exists.
      5. Determines the version based on counts_dict.
      6. Uploads the file to the target S3 location with the hash and version in the filename.
      7. Gathers file metadata to construct a new inventory row.

    Args:
        file_tuple (tuple): A tuple of (file_date, source_file_path).

    Returns:
        Row or None: A pyspark.sql.Row containing the new inventory entry if successful,
        or None if the file was skipped or an error occurred.
    """
    try:
        start = time.time()
        LOGGER.debug(f"Processing {source_file_path}")
        source_file_binary = copy_file_to_mem(source_file_path)
        LOGGER.debug(f"Downloaded in {time.time() - start:.2f}s")

        start = time.time()
        # Validate the NetCDF file
        if not is_netcdf_file_valid(source_file_binary, source_file_path):
            LOGGER.error(f"File {source_file_path} appears corrupt; skipping")
            return None
        LOGGER.debug(f"Validation took {time.time() - start:.2f}s")

        start = time.time()
        # Compute file hash
        file_hash = hash_bytes(source_file_binary)
        LOGGER.debug(f"Hash computed in {time.time() - start:.2f}s")

        # Skip if file already ingested
        if file_hash in existing_hashes:
            LOGGER.info(f"File with hash {file_hash} already ingested; skipping {source_file_path}")
            return None

        start = time.time()
        # Determine version: count of previous copies + 1
        version = counts_dict[source_file_path] + 1
        LOGGER.debug(f"Version computed in {time.time() - start:.2f}s")

        start = time.time()
        # Construct the target file path (and S3 key for upload)
        name, ext = os.path.splitext(os.path.basename(source_file_path))
        active_file_path = os.path.join(target_directory, f"{name}_v{version}_{file_hash}{ext}")
        target_s3_bucket_path = urlparse(target_s3_bucket_path).netloc
        target_file_prefix = urlparse(target_s3_bucket_path).path.lstrip('/')
        target_key = os.path.join(target_file_prefix, os.path.basename(active_file_path))

        # Upload the file to the target S3 location
        result = s3.upload_fileobj(io.BytesIO(source_file_binary), bucket_id, target_key)
        try:
            response = s3.head_object(Bucket=target_s3_bucket_path, Key=target_key)
            print("Object exists:", response)
        except Exception as e:
            print("Object not found or permission error:", e)
        LOGGER.debug(f"result: {result} Uploaded {source_file_path} to {active_file_path} in {time.time() - start:.2f}s")

        start = time.time()
        # Get modification time - assuming source_file_path is accessible locally
        modification_time = os.path.getmtime(source_file_path)
        source_modified_at = datetime.datetime.fromtimestamp(modification_time)
        ingested_at = datetime.datetime.now()
        LOGGER.debug(f"File info obtained in {time.time() - start:.2f}s")

        # Create a new inventory row
        new_entry = Row(
            ingested_at=ingested_at,
            source_file_path=source_file_path,
            file_hash=file_hash,
            active_file_path=active_file_path,
            source_modified_at=source_modified_at,
            data_date=file_date,
        )
        return new_entry
    except Exception as e:
        LOGGER.error(f"Error processing {source_file_path}: {e}")
        return None


def main():
    """Entrypoint."""
    global_start_time = time.time()
    schema_fqdn_path = get_catalog_schema_fqdn()
    create_schema_if_not_exists(schema_fqdn_path)
    target_volume_fqdn_path = f"{schema_fqdn_path}.{ERA5_STAGING_VOLUME_ID}"
    LOGGER.debug(f"create a volume at {target_volume_fqdn_path}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume_fqdn_path}")

    target_volume_location = get_unity_volume_location(target_volume_fqdn_path)
    target_bucket_id = urlparse(target_volume_location).netloc
    target_file_prefix = urlparse(target_volume_location).path.lstrip('/')
    LOGGER.debug(target_file_prefix)
    
    
    target_directory = os.path.join(
        "/Volumes", target_volume_fqdn_path.replace(".", "/")
    )

    target_s3_bucket_path = get_unity_volume_location(target_volume_fqdn_path)

    LOGGER.warning(f"volume created, target directory is: {target_directory}, bucket location is {target_s3_bucket_path}")

    table_definition = load_table_struct(
        ERA5_INVENTORY_TABLE_DEFINITION_PATH, ERA5_INVENTORY_TABLE_NAME
    )
    inventory_table_fqdn = f"{schema_fqdn_path}.{ERA5_INVENTORY_TABLE_NAME}"
    LOGGER.debug(f"creating {inventory_table_fqdn}")
    create_table(inventory_table_fqdn, table_definition)
    LOGGER.debug(f"created {inventory_table_fqdn} successfully")
    latest_date_query = f"""
        SELECT MAX(data_date) AS latest_date
        FROM {inventory_table_fqdn}
    """
    latest_date_df = spark.sql(latest_date_query)
    latest_date = latest_date_df.collect()[0]["latest_date"]
    LOGGER.debug(
        f"latest date seen in the query of {inventory_table_fqdn} is {latest_date}"
    )
    if latest_date is None:
        latest_date = ERA5_START_DATE
    LOGGER.debug(f"working date is {latest_date}")

    start_date = latest_date - relativedelta(months=DELTA_MONTHS)
    end_date = datetime.date.today()

    start_time = time.time()
    source_directory = os.path.join(ERA5_SOURCE_VOLUME_PATH, "daily_summary")
    LOGGER.debug(f"about to search {source_directory}")

    # This is the hard-coded pattern for era5 daily
    pattern = re.compile(r"reanalysis-era5-sfc-daily-(\d{4}-\d{2}-\d{2})\.nc$")
    # sorting here in case the job doesn't complete we will have been working from the 
    # oldest date to the newest date, so querying the database won't kick us too far
    # forward in time if we haven't finished the past.
    # TODO: just have this do ALL the missing dates and also re-test 3 months prior

    era5_source_s3_bucket_path = get_unity_volume_location(ERA5_SOURCE_VOLUME_FQDN)
    aws_access_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_ACCESS_KEY_ID)
    aws_secret_key = dbutils.secrets.get(scope=AWS_SECRET_SCOPE, key=AWS_SECRET_KEY_ID)
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )

    era5_bucket_id = urlparse(era5_source_s3_bucket_path).netloc
    era5_file_prefix = urlparse(era5_source_s3_bucket_path).path.lstrip('/')
    LOGGER.debug(start_date)
    LOGGER.debug(end_date)

    """
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_id, Prefix=era5_file_prefix)

    all_objects = []
    for page in page_iterator:
        if 'Contents' in page:
            all_objects.extend(page['Contents'])

    filtered_files = sorted([
        (file_date, s3_object['Key']) for s3_object in all_objects
        if s3_object['Key'].endswith(".nc")
        and (match := pattern.search(os.path.basename(s3_object['Key'])))
        and (file_date := datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date())
        and start_date <= file_date <= end_date
    ])
    """
    filtered_files = sorted([
        (file_date, file_path)
        for file_path in glob.glob(os.path.join(source_directory, "*.nc"))
        if (match := pattern.search(os.path.basename(file_path)))
        and (
            file_date := datetime.datetime.strptime(
                match.group(1), "%Y-%m-%d"
            ).date()
        )
        and start_date <= file_date <= end_date
    ])
    
    #LOGGER.debug(filtered_files)
    LOGGER.debug(
        f"filtered {len(filtered_files)} in {time.time()-start_time:.2f}s"
    )

    LOGGER.debug(f'about to pre-cache all the file hashes in {inventory_table_fqdn}')
    start = time.time()
    existing_hashes_df = spark.sql(f"SELECT file_hash FROM {inventory_table_fqdn}")
    existing_hashes = {row.file_hash for row in existing_hashes_df.collect()}
    LOGGER.debug(f'took {time.time()-start:.2f}s to create source to hash count')
    start = time.time()
    
    counts_df = spark.sql(
        f"""
        SELECT source_file_path, COUNT(*) as cnt 
        FROM {inventory_table_fqdn}
        GROUP BY source_file_path
        """
    )
    # it's faster to create a lookup in one shot rather than individual calls
    counts_dict = collections.defaultdict(
        int, {row["source_file_path"]: row["cnt"] for row in counts_df.collect()})
    LOGGER.debug(f'took {time.time()-start:.2f}s to create database lookups')

    new_entries = []
    for file_date, source_file_path in tqdm(
        filtered_files, desc="Ingesting files"
    ):
        process_file(s3, era5_bucket_id, file_date, source_file_path, target_directory, existing_hashes, counts_dict, new_entries, era5_file_prefix, target_s3_bucket_path)
        break
    
    # last pass through, just dump the rest not entered
    LOGGER.debug(f"new entries: {new_entries}")
    if new_entries:
        new_df = spark.createDataFrame(new_entries)
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )
        new_entries = []
    LOGGER.info(f"ALL DONE! took {time.time()-global_start_time:.2f}s")


if __name__ == "__main__":
    main()
