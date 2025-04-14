"""ERA5 source to staging pipeline."""

import shutil
import datetime
from dateutil.relativedelta import relativedelta
import logging
import os
import re
import time
import collections
from concurrent.futures import ThreadPoolExecutor

from config import LOCAL_EPHEMERAL_PATH
from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
from config import ERA5_INVENTORY_TABLE_NAME
from config import ERA5_SOURCE_VOLUME_PATH
from config import ERA5_STAGING_VOLUME_ID
from databricks.sdk.runtime import spark
from pyspark.sql import Row

from utils.catalog_support import get_catalog_schema_fqdn
from utils.catalog_support import create_schema_if_not_exists
from utils.file_utils import hash_file
from utils.file_utils import is_netcdf_file_valid
from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType


try:
    dbutils  # Check if dbutils is defined
except NameError:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


# data starts here and we'll use it to set a threshold for when the data should
# be pulled
ERA5_START_DATE = datetime.datetime(2024, 4, 4).date()
DELTA_MONTHS = 3  # always search at least 3 months prior


def process_file_node_batch(
    files_to_process,
    process_file_args,
    inventory_table_fqdn,
):
    """Processes a batch of files concurrently on a single node.

    Used to distribute file processing on a single node to maximize IO
    throughput since Spark can at most launch one job per one CPU instance.
    Processes files through `process_file` function, then results are
    sent to the `inventory_table_fqdn` table.

    Args:
        files_to_process (list): A list containing file metadata or paths to be
            processed.
        process_file_args (dict): Dictionary of arguments to pass to
            `process_file`.
        inventory_table_fqdn (str): Fully-qualified Delta table name
            (database.table) for recording processed file metadata.

    Returns:
        None
    """
    start = time.time()
    with ThreadPoolExecutor(
        max_workers=min(len(files_to_process), 4)
    ) as executor:
        process_file_futures = [
            executor.submit(
                process_file, **dict(process_file_args, file_info=file_info)
            )
            for file_info in files_to_process
        ]

    new_inventory_entries = [
        future.result()
        for future in process_file_futures
        # will be None if the file already exists in the inventory
        if future.result() is not None
    ]

    return new_inventory_entries


def process_file(
    file_info,
    local_directory,
    target_directory,
    existing_hash_dict,
    ingested_file_count_dict,
):
    """Process file: copy locally, validate, hash, and copy to target.

    This function copies a file from its source location to a local directory,
    validates that it is a valid NetCDF file, computes its SHA-256 hash, and
    constructs a new filename incorporating a version number (determined by
    the count of previous ingestions) and the computed hash. If the file is
    invalid or has already been ingested (as determined by existing_hash_dict),
    an error dictionary is returned. Otherwise, the file is copied to the
    target directory and a pyspark.sql.Row containing the file's metadata is
    returned.

    Args:
        file_info (dict): Dictionary with file metadata. Must contain at least:
            - "path": Source file path.
            - "file_modification_time": The last modification time of the file.
            - "file_date": The date associated with the file.
        local_directory (str): Directory path for temporary local storage.
        target_directory (str): Destination directory where the processed
            file is archived.
        existing_hash_dict (dict): Mapping of file hashes to indicate files
            that have already been ingested.
        ingested_file_count_dict (dict): Mapping from source file paths to
            their ingestion counts, used to determine file version.

    Returns:
        pyspark.sql.Row or dict: Returns a pyspark.sql.Row containing file
            metadata if processing is successful; otherwise, returns a
            dictionary with an "error" key describing the issue encountered.
    """
    try:
        LOGGER.debug(f"current file info: {file_info}")
        source_file_path = file_info["path"]
        LOGGER.debug(f"Processing {source_file_path}")
        local_file_path = os.path.join(
            local_directory, os.path.basename(source_file_path)
        )
        if os.path.exists(local_file_path):
            # This could happen during sandboxing with a leftover file
            os.remove(local_file_path)
        shutil.copyfile(source_file_path, local_file_path)
        LOGGER.debug(f"Copied {local_file_path}")

        # Determine the file version defined as the count of previous copies+1
        version = ingested_file_count_dict[source_file_path] + 1
        name, ext = os.path.splitext(os.path.basename(source_file_path))
        file_hash = hash_file(local_file_path)
        LOGGER.debug(f"Hash computed on {local_file_path}")

        if not is_netcdf_file_valid(local_file_path):
            LOGGER.error(f"File {source_file_path} appears corrupt; skipping")
            return {
                "error": (
                    f"File {source_file_path} to {local_file_path} appears "
                    f"corrupt; skipping"
                )
            }
        LOGGER.debug(f"Validation complete for {local_file_path}")

        # Skip if file already ingested
        if file_hash in existing_hash_dict:
            LOGGER.info(
                f"File with hash {file_hash} already ingested; skipping "
                f"{source_file_path}"
            )
            return None

        # file is valid, copy it to target
        active_file_path = os.path.join(
            target_directory, f"{name}_v{version}_{file_hash}{ext}"
        )
        # copy it from the local because that's an NVME and the original
        # source is a goofys mounted s3 bucket
        shutil.copyfile(local_file_path, active_file_path)
        ingested_at = datetime.datetime.now()
        LOGGER.debug(
            f"File copied from {local_file_path} to {active_file_path}"
        )

        # Create a inventory entry for this file
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
        LOGGER.exception(f"Error processing {source_file_path}: {e}")
        raise
    finally:
        try:
            os.remove(local_file_path)
        except Exception:
            # This shouldn't happen but it's okay if it does since local
            # storage is ephemeral
            LOGGER.exception(f"could not remove {local_file_path}, skipping")


def main():
    """Entrypoint."""
    global_start_time = time.time()
    spark = SparkSession.builder.getOrCreate()
    schema_fqdn_path = get_catalog_schema_fqdn()
    LOGGER.info(f"Create a schema at {schema_fqdn_path} if not exists")
    create_schema_if_not_exists(schema_fqdn_path)
    target_volume_fqdn_path = f"{schema_fqdn_path}.{ERA5_STAGING_VOLUME_ID}"
    LOGGER.info(f"Create a volume at {target_volume_fqdn_path} if not exists")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume_fqdn_path}")

    target_directory = os.path.join(
        "/Volumes", target_volume_fqdn_path.replace(".", "/")
    )

    LOGGER.info(f"Target directory is: {target_directory}")

    inventory_table_sql_schema = load_table_struct(
        ERA5_INVENTORY_TABLE_DEFINITION_PATH, ERA5_INVENTORY_TABLE_NAME
    )
    inventory_table_fqdn = f"{schema_fqdn_path}.{ERA5_INVENTORY_TABLE_NAME}"
    LOGGER.info(f"Creating inventory table at {inventory_table_fqdn}")
    create_table(inventory_table_fqdn, inventory_table_sql_schema)

    now = datetime.datetime.now()
    test_entry = Row(
        ingested_at=now,
        source_file_path="TEST",
        file_hash="TEST",
        active_file_path="TEST",
        source_modified_at=now,
        data_date=now.date(),
    )
    test_df = spark.createDataFrame([test_entry])
    test_df.write.format('delta').mode('append').saveAsTable(inventory_table_fqdn)

    LOGGER.info(f"Query most recent date from {inventory_table_fqdn}")
    latest_date_query = f"""
        SELECT MAX(data_date) AS latest_date
        FROM {inventory_table_fqdn}
    """
    latest_date_df = spark.sql(latest_date_query)
    latest_date = latest_date_df.collect()[0]["latest_date"]
    LOGGER.info(
        f"latest date seen in the query of "
        f"{inventory_table_fqdn} is {latest_date}"
    )
    if latest_date is None:
        latest_date = ERA5_START_DATE
    LOGGER.info(f"working date is {latest_date}")

    start_date = latest_date - relativedelta(months=DELTA_MONTHS)
    end_date = datetime.date.today()

    start_time = time.time()
    source_directory = os.path.join(ERA5_SOURCE_VOLUME_PATH, "daily_summary")
    LOGGER.info(f"about to search {source_directory}")

    # This is the hard-coded pattern for era5 daily netcdf files
    pattern = re.compile(r"reanalysis-era5-sfc-daily-(\d{4}-\d{2}-\d{2})\.nc$")

    LOGGER.info(f"search for files between {start_date} and {end_date}")
    files_to_process = sorted(
        [
            {
                "file_date": file_date,
                # dbfs ls gives us prefixed with dbfs// so we strip it here
                "path": file_info.path.strip("dbfs:"),
                # dbutils.fs does time in ms, so convert to seconds w/ / 1000
                "file_modification_time": datetime.datetime.fromtimestamp(
                    file_info.modificationTime / 1000
                ),
            }
            # prefix with dbfs: because it's faster
            for file_info in dbutils.fs.ls(f"dbfs:{source_directory}")
            if (match := pattern.search(os.path.basename(file_info.path)))
            and (  # noqa: W503
                file_date := datetime.datetime.strptime(
                    match.group(1), "%Y-%m-%d"
                ).date()
            )
            and start_date <= file_date <= end_date  # noqa: W503
        ][
            :1
        ],  # TODO: put [0] here for debugging
        key=lambda x: x["file_date"],
    )
    LOGGER.info(
        f"filtered {len(files_to_process)} in {time.time()-start_time:.2f}s"
    )

    # it's faster to create these file hash and version count lookups in
    # one shot rather than individual calls to the database
    LOGGER.info(f"Get existing file hashes from {inventory_table_fqdn}")
    existing_hashes_df = spark.sql(
        f"SELECT file_hash FROM {inventory_table_fqdn}"
    )
    existing_hash_dict = {row.file_hash for row in existing_hashes_df.collect()}

    LOGGER.info(f"Get existing file count from {inventory_table_fqdn}")
    counts_df = spark.sql(
        f"""
        SELECT source_file_path, COUNT(*) as cnt
        FROM {inventory_table_fqdn}
        GROUP BY source_file_path
        """
    )
    ingested_file_count_dict = collections.defaultdict(
        int,
        {row["source_file_path"]: row["cnt"] for row in counts_df.collect()},
    )

    # I tested this manually and found 4 jobs per cpu is good for the goofys
    # file deamon throughput
    jobs_per_cpu = 4
    batches_to_process = [
        files_to_process[i : i + jobs_per_cpu]  # noqa: E203
        for i in range(0, len(files_to_process), jobs_per_cpu)
    ]

    process_file_node_batch_udf = udf(
        process_file_node_batch, ArrayType(inventory_table_sql_schema)
    )

    file_batch_df = spark.createDataFrame(
        [(b,) for b in batches_to_process], ["batch"]
    )

    start = time.time()
    new_inventory_entry_lists = process_file_node_batch_udf(col("batch"))
    LOGGER.info(
        f"ALL DONE! took {time.time()-global_start_time:.4f}s {(time.time()-start)/len(files_to_process):.2f}s per file"
    )
    if new_inventory_entry_lists:
        new_df = spark.createDataFrame(new_inventory_entry_lists)
        LOGGER.debug(new_inventory_entry_lists)
        LOGGER.debug(inventory_table_fqdn)
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )
        

if __name__ == "__main__":
    main()
