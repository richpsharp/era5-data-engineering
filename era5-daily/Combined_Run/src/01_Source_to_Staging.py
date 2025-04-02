"""ERA5 source to staging pipeline."""

import datetime
from dateutil.relativedelta import relativedelta
import glob
import logging
import os
import re
import time

from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
from config import ERA5_INVENTORY_TABLE_NAME
from config import ERA5_SOURCE_VOLUME_PATH
from config import ERA5_STAGING_VOLUME_ID
from databricks.sdk.runtime import spark
from pyspark.sql import Row
from utils.catalog_support import get_catalog_schema_fqdn
from utils.file_utils import copy_file_to_mem
from utils.file_utils import hash_bytes
from utils.file_utils import copy_mem_file_to_path

from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct
import xarray as xr

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
    LOGGER.debug("all done")
    latest_date_query = f"""
        SELECT MAX(data_date) AS latest_date
        FROM {inventory_table_fqdn}
    """
    latest_date_df = spark.sql(latest_date_query)
    latest_date = latest_date_df.collect()[0]["latest_date"]
    if latest_date is None:
        latest_date = ERA5_START_DATE
    LOGGER.debug(latest_date)

    start_date = (latest_date - relativedelta(months=DELTA_MONTHS))
    end_date = datetime.date.today()

    # This is the hard-coded pattern for era5 daily
    start_time = time.time()
    source_directory = os.path.join(ERA5_SOURCE_VOLUME_PATH, "daily_summary")
    LOGGER.debug(f"about to search {source_directory}")

    pattern = re.compile(r"reanalysis-era5-sfc-daily-(\d{4}-\d{2}-\d{2})\.nc$")
    filtered_files = [
        file_path
        for file_path in glob.glob(os.path.join(source_directory, "*.nc"))
        if (match := pattern.search(os.path.basename(file_path)))
        and (
            file_date := datetime.datetime.strptime(
                match.group(1), "%Y-%m-%d"
            ).date()
        )
        and start_date <= file_date <= end_date
    ]
    LOGGER.debug(
        f"filtered {len(filtered_files)} in {time.time()-start_time:.2f}s"
    )

    for source_file_path in filtered_files:
        start = time.time()
        source_file_binary = copy_file_to_mem(source_file_path)
        file_hash = hash_bytes(source_file_binary)

        # Skip if an entry with this file hash already exists
        existing = spark.sql(
            f"SELECT 1 FROM {inventory_table_fqdn} "
            f"WHERE file_hash = '{file_hash}' LIMIT 1"
        )
        if existing.count() > 0:
            LOGGER.info(
                f"File with hash {file_hash} already ingested; "
                f"skipping {source_file_path}"
            )
            continue

        # set the version number of the target file equal to 1 plus
        # however many previous copies were made (with different hashes)
        base_count_df = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {inventory_table_fqdn} "
            f"WHERE source_file_path = '{source_file_path}'"
        )
        base_count = base_count_df.collect()[0]["cnt"]
        version = base_count + 1

        # put in target directory and inject the hash
        active_file_path = os.path.join(
            target_directory,
            f"%s_v{version}_{file_hash}%s"
            % os.path.splitext(os.path.basename(source_file_path)),
        )
        copy_mem_file_to_path(source_file_binary, active_file_path)

        LOGGER.info(
            f"copied {source_file_path} to {active_file_path} in "
            f"{time.time()-start:.2f}s"
        )

        file_info = dbutils.fs.ls(source_file_path)[0]
        # dbutils provides modification time in ms, /1000 to convert to sec
        # since datetime expects it as such
        source_modified_at = datetime.datetime.fromtimestamp(
            file_info.modificationTime / 1000
        )
        ingested_at = datetime.datetime.now()
        new_entry = Row(
            ingested_at=ingested_at,
            source_file_path=source_file_path,
            file_hash=file_hash,
            active_file_path=active_file_path,
            source_modified_at=source_modified_at,
            data_date=file_date,
        )
        new_df = spark.createDataFrame([new_entry])
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )

    LOGGER.info(f"ALL DONE! took {global_start_time-time.time():.2f}s")


if __name__ == "__main__":
    main()
