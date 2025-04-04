"""ERA5 source to staging pipeline."""

import datetime
from dateutil.relativedelta import relativedelta
import logging
import os
import re
import time
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import LOCAL_EPHEMERAL_PATH
from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
from config import ERA5_INVENTORY_TABLE_NAME
from config import ERA5_SOURCE_VOLUME_PATH
from config import ERA5_STAGING_VOLUME_ID
from databricks.sdk.runtime import spark
from pyspark.sql import Row
from tqdm import tqdm
from utils.catalog_support import get_catalog_schema_fqdn
from utils.catalog_support import create_schema_if_not_exists
from utils.file_utils import hash_file
from utils.file_utils import is_netcdf_file_valid
from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct

try:
    dbutils  # Check if dbutils is defined
except NameError:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


# data starts here and we'll use it to set a threshold for when the data should
# be pulled
ERA5_START_DATE = datetime.datetime(1950, 1, 1).date()
DELTA_MONTHS = 3  # always search at least 3 months prior


def process_file(
    file_info,
    local_directory,
    target_directory,
    existing_hash_dict,
    ingested_file_count_dict,
):
    """Process a single file: download from S3, validate, hash, and re-upload.

    This function performs the following steps:
      1. Downloads the file from S3.
      2. Validates the NetCDF file using xarray.
      3. Computes its SHA-256 hash.
      4. Skips processing if the hash already exists.
      5. Determines the version based on ingested_file_count_dict.
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
        source_file_path = file_info["path"]
        LOGGER.debug(f"Processing {source_file_path}")
        local_file_path = os.path.join(
            local_directory, os.path.basename(source_file_path)
        )
        LOGGER.debug(f"copying from {source_file_path} to {local_file_path}")
        # dbutils needs to know this is a local file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        dbutils.fs.cp(source_file_path, f"file://{local_file_path}")
        LOGGER.debug(f"Downloaded in {time.time() - start:.2f}s")

        start = time.time()
        if not is_netcdf_file_valid(local_file_path):
            LOGGER.error(f"File {source_file_path} appears corrupt; skipping")
            return None
        LOGGER.debug(f"Validation took {time.time() - start:.2f}s")

        start = time.time()
        # Compute file hash
        file_hash = hash_file(local_file_path)
        try:
            os.remove(local_file_path)
        except Exception:
            LOGGER.exception(f"could not remove {local_file_path}, skipping")
            pass
        LOGGER.debug(f"Hash computed in {time.time() - start:.2f}s")

        # Skip if file already ingested
        if file_hash in existing_hash_dict:
            LOGGER.info(
                f"File with hash {file_hash} already ingested; skipping "
                f"{source_file_path}"
            )
            return None

        start = time.time()
        # Determine the file version defined as the count of previous copies+1
        version = ingested_file_count_dict[source_file_path] + 1
        name, ext = os.path.splitext(os.path.basename(source_file_path))
        active_file_path = os.path.join(
            target_directory, f"{name}_v{version}_{file_hash}{ext}"
        )

        # note this is copying the *source* to the active, not the local
        # hopefully taking advantage of databrick's ability to copy from
        # one bucket to another quickly
        start = time.time()
        # now were copying AWAY from the local file system
        dbutils.fs.cp(source_file_path, f"dbfs://{active_file_path}")
        ingested_at = datetime.datetime.now()
        LOGGER.debug(f"File copied in {time.time() - start:.2f}s")

        # Create a new inventory row
        new_entry = Row(
            ingested_at=ingested_at,
            source_file_path=source_file_path,
            file_hash=file_hash,
            active_file_path=active_file_path,
            source_modified_at=file_info["file_modification_time"],
            data_date=file_info["file_date"],
        )
        return new_entry
    except Exception as e:
        LOGGER.error(f"Error processing {source_file_path}: {e}")
        return None


def build_file_info(file_info, pattern, start_date, end_date):
    """Process a single FileInfo object from dbutils.fs.ls.

    Args:
        file_info: A FileInfo object with attributes .path, .modificationTime, etc.

    Returns:
        dict or None: A dictionary with file_date, path, and file_modification_time,
            or None if the file doesn't match the filter criteria.
    """
    filename = os.path.basename(file_info.path)
    match = pattern.search(filename)
    if not match:
        return None
    try:
        file_date = datetime.datetime.strptime(
            match.group(1), "%Y-%m-%d"
        ).date()
    except Exception as e:
        return None
    if not (start_date <= file_date <= end_date):
        return None

    return {
        "file_date": file_date,
        "path": file_info.path,
        "file_modification_time": datetime.datetime.fromtimestamp(
            file_info.modificationTime / 1000
        ),
    }


def main():
    """Entrypoint."""
    global_start_time = time.time()
    schema_fqdn_path = get_catalog_schema_fqdn()
    create_schema_if_not_exists(schema_fqdn_path)
    target_volume_fqdn_path = f"{schema_fqdn_path}.{ERA5_STAGING_VOLUME_ID}"
    LOGGER.debug(f"create a volume at {target_volume_fqdn_path}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume_fqdn_path}")

    target_directory = os.path.join(
        "/Volumes", target_volume_fqdn_path.replace(".", "/")
    )

    LOGGER.warning(f"volume created, target directory is: {target_directory}")

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
        f"latest date seen in the query of "
        f"{inventory_table_fqdn} is {latest_date}"
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

    LOGGER.debug(f"search for files between {start_date} and {end_date} v3")
    files_to_process = sorted(
        [
            {
                "file_date": file_date,
                "path": file_info.path,
                # dbutils.fs does time in ms, so convert to seconds w/ / 1000
                "file_modification_time": datetime.datetime.fromtimestamp(
                    file_info.modificationTime / 1000
                ),
            }
            for file_info in dbutils.fs.ls(source_directory)
            if (match := pattern.search(os.path.basename(file_info.path)))
            and (  # noqa: W503
                file_date := datetime.datetime.strptime(
                    match.group(1), "%Y-%m-%d"
                ).date()
            )
            and start_date <= file_date <= end_date  # noqa: W503
        ],
        key=lambda x: x["file_date"],
    )
    LOGGER.debug(
        f"filtered {len(files_to_process)} in {time.time()-start_time:.2f}s"
    )

    start = time.time()
    existing_hashes_df = spark.sql(
        f"SELECT file_hash FROM {inventory_table_fqdn}"
    )
    existing_hash_dict = {row.file_hash for row in existing_hashes_df.collect()}
    counts_df = spark.sql(
        f"""
        SELECT source_file_path, COUNT(*) as cnt 
        FROM {inventory_table_fqdn}
        GROUP BY source_file_path
        """
    )
    # it's faster to create a lookup in one shot rather than individual calls
    ingested_file_count_dict = collections.defaultdict(
        int,
        {row["source_file_path"]: row["cnt"] for row in counts_df.collect()},
    )
    LOGGER.debug(f"took {time.time()-start:.2f}s to create database lookups")

    new_entries = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        process_file_futures = [
            executor.submit(
                process_file,
                file_info,
                LOCAL_EPHEMERAL_PATH,
                target_directory,
                existing_hash_dict,
                ingested_file_count_dict,
            )
            for file_info in files_to_process[0:10]
        ]

    new_entries = []
    for future in tqdm(as_completed(process_file_futures), total=len(process_file_futures), desc="Ingesting files"):
        result = future.result()
        if result is not None:
            new_entries.append(result)

    # last pass through, just dump the rest not entered
    LOGGER.debug(f"new entries: {new_entries}")
    if new_entries:
        new_df = spark.createDataFrame(new_entries)
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )
    LOGGER.info(f"ALL DONE! took {time.time()-global_start_time:.2f}s")


if __name__ == "__main__":
    main()
