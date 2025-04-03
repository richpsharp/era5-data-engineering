"""ERA5 source to staging pipeline."""

import datetime
from dateutil.relativedelta import relativedelta
import glob
import logging
import os
import re
import time
import collections

from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
from config import ERA5_INVENTORY_TABLE_NAME
from config import ERA5_SOURCE_VOLUME_PATH
from config import ERA5_STAGING_VOLUME_ID
from databricks.sdk.runtime import spark
from pyspark.sql import Row
from tqdm import tqdm
from utils.catalog_support import get_catalog_schema_fqdn
from utils.catalog_support import create_schema_if_not_exists
from utils.file_utils import copy_file_to_mem
from utils.file_utils import copy_mem_file_to_path
from utils.file_utils import hash_bytes
from utils.file_utils import is_netcdf_file_valid
from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# data starts here and we'll use it to set a threshold for when the data should
# be pulled
ERA5_START_DATE = datetime.datetime(1950, 1, 1).date()
DELTA_MONTHS = 3  # always search at least 3 months prior


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
    LOGGER.debug(f"volume created, target directory is: {target_directory}")

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
        start = time.time()
        source_file_binary = copy_file_to_mem(source_file_path)
        LOGGER.debug(f'took {time.time()-start:.2f}s to copy file to mem')
        start = time.time()
        if not is_netcdf_file_valid(source_file_binary, source_file_path):
            LOGGER.error(
                f"Could not open {source_file_path} with xarray, might be "
                f"corrupt, skipping"
            )
            continue
        LOGGER.debug(f'took {time.time()-start:.2f}s to test if netcdf is valid')
        start = time.time()
        file_hash = hash_bytes(source_file_binary)
        LOGGER.debug(f'took {time.time()-start:.2f}s to calculate file hash')
        start = time.time()
        if file_hash in existing_hashes:
            LOGGER.info(
                f"File with hash {file_hash} already ingested; "
                f"skipping {source_file_path}"
            )
            continue
        LOGGER.debug(f'took {time.time()-start:.2f}s to test file hash')
        start = time.time()
        
        # file version is how many previous copies of the file exist + 1
        version = counts_dict[source_file_path]  + 1
        LOGGER.debug(f'took {time.time()-start:.2f}s to calculate previous inventory count')
        start = time.time()

        # put in target directory and inject the hash
        active_file_path = os.path.join(
            target_directory,
            f"%s_v{version}_{file_hash}%s"
            % os.path.splitext(os.path.basename(source_file_path)),
        )
        copy_mem_file_to_path(source_file_binary, active_file_path)
        LOGGER.info(
            f"took {time.time()-start:.2f}s to copy {source_file_path} to {active_file_path}")
        start = time.time()
        
        file_info = dbutils.fs.ls(source_file_path)[0]
        # dbutils provides modification time in ms, /1000 to convert to sec
        # since datetime expects it as such
        source_modified_at = datetime.datetime.fromtimestamp(
            file_info.modificationTime / 1000
        )
        ingested_at = datetime.datetime.now()
        LOGGER.debug(f'took {time.time()-start:.2f}s to determine file info')
        start = time.time()
        
        new_entry = Row(
            ingested_at=ingested_at,
            source_file_path=source_file_path,
            file_hash=file_hash,
            active_file_path=active_file_path,
            source_modified_at=source_modified_at,
            data_date=file_date,
        )
        new_entries.append(new_entry)
        if len(new_entries) > 100:
            new_df = spark.createDataFrame(new_entries)
            new_df.write.format("delta").mode("append").saveAsTable(
                inventory_table_fqdn
            )
            new_entries = []
        LOGGER.debug(f'took {time.time()-start:.2f}s to write new row into delta table')
    
    # last pass through, just dump the rest not entered
    if new_entries:
        new_df = spark.createDataFrame(new_entries)
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )
        new_entries = []
    LOGGER.info(f"ALL DONE! took {time.time()-global_start_time:.2f}s")


if __name__ == "__main__":
    main()
