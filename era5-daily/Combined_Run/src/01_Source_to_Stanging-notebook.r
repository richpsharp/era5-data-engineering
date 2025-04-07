# Databricks notebook source
# MAGIC %python
# MAGIC """ERA5 source to staging pipeline."""
# MAGIC
# MAGIC import shutil
# MAGIC import datetime
# MAGIC from dateutil.relativedelta import relativedelta
# MAGIC import logging
# MAGIC import os
# MAGIC import re
# MAGIC import time
# MAGIC import collections
# MAGIC from concurrent.futures import ThreadPoolExecutor
# MAGIC
# MAGIC from config import LOCAL_EPHEMERAL_PATH
# MAGIC from config import ERA5_INVENTORY_TABLE_DEFINITION_PATH
# MAGIC from config import ERA5_INVENTORY_TABLE_NAME
# MAGIC from config import ERA5_SOURCE_VOLUME_PATH
# MAGIC from config import ERA5_STAGING_VOLUME_ID
# MAGIC from databricks.sdk.runtime import spark
# MAGIC from pyspark.sql import Row
# MAGIC
# MAGIC from tqdm import tqdm
# MAGIC from utils.catalog_support import get_catalog_schema_fqdn
# MAGIC from utils.catalog_support import create_schema_if_not_exists
# MAGIC from utils.file_utils import hash_file
# MAGIC from utils.file_utils import is_netcdf_file_valid
# MAGIC from utils.table_definition_loader import create_table
# MAGIC from utils.table_definition_loader import load_table_struct
# MAGIC from pyspark import SparkContext
# MAGIC
# MAGIC try:
# MAGIC     dbutils  # Check if dbutils is defined
# MAGIC except NameError:
# MAGIC     from pyspark.dbutils import DBUtils
# MAGIC
# MAGIC     dbutils = DBUtils(spark)
# MAGIC
# MAGIC LOGGER = logging.getLogger(__name__)
# MAGIC LOGGER.setLevel(logging.DEBUG)
# MAGIC
# MAGIC
# MAGIC # data starts here and we'll use it to set a threshold for when the data should
# MAGIC # be pulled
# MAGIC ERA5_START_DATE = datetime.datetime(1950, 1, 1).date()
# MAGIC DELTA_MONTHS = 3  # always search at least 3 months prior
# MAGIC
# MAGIC
# MAGIC def process_file_node_batch(
# MAGIC     files_to_process,
# MAGIC     process_file_args,
# MAGIC     inventory_table_fqdn,
# MAGIC ):
# MAGIC     """Processes a batch of files concurrently on a single node.
# MAGIC
# MAGIC     Used to distribute file processing on a single node to maximize IO
# MAGIC     throughput since Spark can at most launch one job per one CPU instance.
# MAGIC     Processes files through `process_file` function, then results are
# MAGIC     sent to the `inventory_table_fqdn` table.
# MAGIC
# MAGIC     Args:
# MAGIC         files_to_process (list): A list containing file metadata or paths to be
# MAGIC             processed.
# MAGIC         process_file_args (dict): Dictionary of arguments to pass to
# MAGIC             `process_file`.
# MAGIC         inventory_table_fqdn (str): Fully-qualified Delta table name
# MAGIC             (database.table) for recording processed file metadata.
# MAGIC
# MAGIC     Returns:
# MAGIC         None
# MAGIC     """
# MAGIC     start = time.time()
# MAGIC     with ThreadPoolExecutor(max_workers=min(len(files_to_process), 4)) as executor:
# MAGIC         process_file_futures = [
# MAGIC             executor.submit(
# MAGIC                 process_file, **dict(process_file_args, file_info=file_info)
# MAGIC             )
# MAGIC             for file_info in files_to_process
# MAGIC         ]
# MAGIC
# MAGIC     new_inventory_entries = [
# MAGIC         future.result()
# MAGIC         for future in process_file_futures
# MAGIC         # will be None if the file already exists in the inventory
# MAGIC         if future.result() is not None
# MAGIC     ]
# MAGIC
# MAGIC     return new_inventory_entries
# MAGIC     LOGGER.debug(f"all done batch in {time.time()-start:.2f}s")
# MAGIC
# MAGIC
# MAGIC def process_file(
# MAGIC     file_info,
# MAGIC     local_directory,
# MAGIC     target_directory,
# MAGIC     existing_hash_dict,
# MAGIC     ingested_file_count_dict,
# MAGIC ):
# MAGIC     """Process file: copy locally, validate, hash, and copy to target.
# MAGIC
# MAGIC     This function copies a file from its source location to a local directory,
# MAGIC     validates that it is a valid NetCDF file, computes its SHA-256 hash, and
# MAGIC     constructs a new filename incorporating a version number (determined by
# MAGIC     the count of previous ingestions) and the computed hash. If the file is
# MAGIC     invalid or has already been ingested (as determined by existing_hash_dict),
# MAGIC     an error dictionary is returned. Otherwise, the file is copied to the
# MAGIC     target directory and a pyspark.sql.Row containing the file's metadata is
# MAGIC     returned.
# MAGIC
# MAGIC     Args:
# MAGIC         file_info (dict): Dictionary with file metadata. Must contain at least:
# MAGIC             - "path": Source file path.
# MAGIC             - "file_modification_time": The last modification time of the file.
# MAGIC             - "file_date": The date associated with the file.
# MAGIC         local_directory (str): Directory path for temporary local storage.
# MAGIC         target_directory (str): Destination directory where the processed
# MAGIC             file is archived.
# MAGIC         existing_hash_dict (dict): Mapping of file hashes to indicate files
# MAGIC             that have already been ingested.
# MAGIC         ingested_file_count_dict (dict): Mapping from source file paths to
# MAGIC             their ingestion counts, used to determine file version.
# MAGIC
# MAGIC     Returns:
# MAGIC         pyspark.sql.Row or dict: Returns a pyspark.sql.Row containing file
# MAGIC             metadata if processing is successful; otherwise, returns a
# MAGIC             dictionary with an "error" key describing the issue encountered.
# MAGIC     """
# MAGIC     try:
# MAGIC         LOGGER.debug(f"current file info: {file_info}")
# MAGIC         source_file_path = file_info["path"]
# MAGIC         LOGGER.debug(f"Processing {source_file_path}")
# MAGIC         local_file_path = os.path.join(
# MAGIC             local_directory, os.path.basename(source_file_path)
# MAGIC         )
# MAGIC         if os.path.exists(local_file_path):
# MAGIC             # This could happen during sandboxing with a leftover file
# MAGIC             os.remove(local_file_path)
# MAGIC         shutil.copyfile(source_file_path, local_file_path)
# MAGIC         LOGGER.debug(f"Copied {local_file_path}")
# MAGIC
# MAGIC         # Determine the file version defined as the count of previous copies+1
# MAGIC         version = ingested_file_count_dict[source_file_path] + 1
# MAGIC         name, ext = os.path.splitext(os.path.basename(source_file_path))
# MAGIC         file_hash = hash_file(local_file_path)
# MAGIC         LOGGER.debug(f"Hash computed on {local_file_path}")
# MAGIC
# MAGIC         if not is_netcdf_file_valid(local_file_path):
# MAGIC             LOGGER.error(f"File {source_file_path} appears corrupt; skipping")
# MAGIC             return {
# MAGIC                 "error": (
# MAGIC                     f"File {source_file_path} to {local_file_path} appears "
# MAGIC                     f"corrupt; skipping"
# MAGIC                 )
# MAGIC             }
# MAGIC         LOGGER.debug(f"Validation complete for {local_file_path}")
# MAGIC
# MAGIC         # Skip if file already ingested
# MAGIC         if file_hash in existing_hash_dict and False:
# MAGIC             LOGGER.info(
# MAGIC                 f"File with hash {file_hash} already ingested; skipping "
# MAGIC                 f"{source_file_path}"
# MAGIC             )
# MAGIC             return None
# MAGIC
# MAGIC         # file is valid, copy it to target
# MAGIC         active_file_path = os.path.join(
# MAGIC             target_directory, f"{name}_v{version}_{file_hash}{ext}"
# MAGIC         )
# MAGIC         # copy it from the local because that's an NVME and the original
# MAGIC         # source is a goofys mounted s3 bucket
# MAGIC         shutil.copyfile(local_file_path, active_file_path)
# MAGIC         ingested_at = datetime.datetime.now()
# MAGIC         LOGGER.debug(
# MAGIC             f"File copied from {local_file_path} to {active_file_path}"
# MAGIC         )
# MAGIC
# MAGIC         # Create a inventory entry for this file
# MAGIC         new_entry = Row(
# MAGIC             ingested_at=ingested_at,
# MAGIC             source_file_path=source_file_path,
# MAGIC             file_hash=file_hash,
# MAGIC             active_file_path=active_file_path,
# MAGIC             source_modified_at=file_info["file_modification_time"],
# MAGIC             data_date=file_info["file_date"],
# MAGIC         )
# MAGIC
# MAGIC         return new_entry
# MAGIC     except Exception as e:
# MAGIC         LOGGER.exception(f"Error processing {source_file_path}: {e}")
# MAGIC         raise
# MAGIC     finally:
# MAGIC         try:
# MAGIC             os.remove(local_file_path)
# MAGIC         except Exception:
# MAGIC             # This shouldn't happen but it's okay if it does since local
# MAGIC             # storage is ephemeral
# MAGIC             LOGGER.exception(f"could not remove {local_file_path}, skipping")
# MAGIC
# MAGIC
# MAGIC def main():
# MAGIC     """Entrypoint."""
# MAGIC     global_start_time = time.time()
# MAGIC     schema_fqdn_path = get_catalog_schema_fqdn()
# MAGIC     LOGGER.info(f"Create a schema at {schema_fqdn_path} if not exists")
# MAGIC     create_schema_if_not_exists(schema_fqdn_path)
# MAGIC     target_volume_fqdn_path = f"{schema_fqdn_path}.{ERA5_STAGING_VOLUME_ID}"
# MAGIC     LOGGER.info(f"Create a volume at {target_volume_fqdn_path} if not exists")
# MAGIC     spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_volume_fqdn_path}")
# MAGIC
# MAGIC     target_directory = os.path.join(
# MAGIC         "/Volumes", target_volume_fqdn_path.replace(".", "/")
# MAGIC     )
# MAGIC
# MAGIC     LOGGER.info(f"Target directory is: {target_directory}")
# MAGIC
# MAGIC     table_definition = load_table_struct(
# MAGIC         ERA5_INVENTORY_TABLE_DEFINITION_PATH, ERA5_INVENTORY_TABLE_NAME
# MAGIC     )
# MAGIC     inventory_table_fqdn = f"{schema_fqdn_path}.{ERA5_INVENTORY_TABLE_NAME}"
# MAGIC     LOGGER.info(f"Creating inventory table at {inventory_table_fqdn}")
# MAGIC     create_table(inventory_table_fqdn, table_definition)
# MAGIC
# MAGIC     LOGGER.info(f"Query most recent date from {inventory_table_fqdn}")
# MAGIC     latest_date_query = f"""
# MAGIC         SELECT MAX(data_date) AS latest_date
# MAGIC         FROM {inventory_table_fqdn}
# MAGIC     """
# MAGIC     latest_date_df = spark.sql(latest_date_query)
# MAGIC     latest_date = latest_date_df.collect()[0]["latest_date"]
# MAGIC     LOGGER.info(
# MAGIC         f"latest date seen in the query of "
# MAGIC         f"{inventory_table_fqdn} is {latest_date}"
# MAGIC     )
# MAGIC     if latest_date is None:
# MAGIC         latest_date = ERA5_START_DATE
# MAGIC     LOGGER.info(f"working date is {latest_date}")
# MAGIC
# MAGIC     start_date = latest_date - relativedelta(months=DELTA_MONTHS)
# MAGIC     end_date = datetime.date.today()
# MAGIC
# MAGIC     start_time = time.time()
# MAGIC     source_directory = os.path.join(ERA5_SOURCE_VOLUME_PATH, "daily_summary")
# MAGIC     LOGGER.info(f"about to search {source_directory}")
# MAGIC
# MAGIC     # This is the hard-coded pattern for era5 daily netcdf files
# MAGIC     pattern = re.compile(r"reanalysis-era5-sfc-daily-(\d{4}-\d{2}-\d{2})\.nc$")
# MAGIC
# MAGIC     LOGGER.info(f"search for files between {start_date} and {end_date}")
# MAGIC     files_to_process = sorted(
# MAGIC         [
# MAGIC             {
# MAGIC                 "file_date": file_date,
# MAGIC                 # dbfs ls gives us prefixed with dbfs// so we strip it here
# MAGIC                 "path": file_info.path.strip("dbfs:"),
# MAGIC                 # dbutils.fs does time in ms, so convert to seconds w/ / 1000
# MAGIC                 "file_modification_time": datetime.datetime.fromtimestamp(
# MAGIC                     file_info.modificationTime / 1000
# MAGIC                 ),
# MAGIC             }
# MAGIC             # prefix with dbfs: because it's faster
# MAGIC             for file_info in dbutils.fs.ls(f"dbfs:{source_directory}")
# MAGIC             if (match := pattern.search(os.path.basename(file_info.path)))
# MAGIC             and (  # noqa: W503
# MAGIC                 file_date := datetime.datetime.strptime(
# MAGIC                     match.group(1), "%Y-%m-%d"
# MAGIC                 ).date()
# MAGIC             )
# MAGIC             and start_date <= file_date <= end_date  # noqa: W503
# MAGIC         ],
# MAGIC         key=lambda x: x["file_date"],
# MAGIC     )
# MAGIC     LOGGER.info(
# MAGIC         f"filtered {len(files_to_process)} in {time.time()-start_time:.2f}s"
# MAGIC     )
# MAGIC
# MAGIC     # it's faster to create these file hash and version count lookups in
# MAGIC     # one shot rather than individual calls to the database
# MAGIC     LOGGER.info(f"Get existing file hashes from {inventory_table_fqdn}")
# MAGIC     existing_hashes_df = spark.sql(
# MAGIC         f"SELECT file_hash FROM {inventory_table_fqdn}"
# MAGIC     )
# MAGIC     existing_hash_dict = {row.file_hash for row in existing_hashes_df.collect()}
# MAGIC
# MAGIC     LOGGER.info(f"Get existing file count from {inventory_table_fqdn}")
# MAGIC     counts_df = spark.sql(
# MAGIC         f"""
# MAGIC         SELECT source_file_path, COUNT(*) as cnt
# MAGIC         FROM {inventory_table_fqdn}
# MAGIC         GROUP BY source_file_path
# MAGIC         """
# MAGIC     )
# MAGIC     ingested_file_count_dict = collections.defaultdict(
# MAGIC         int,
# MAGIC         {row["source_file_path"]: row["cnt"] for row in counts_df.collect()},
# MAGIC     )
# MAGIC
# MAGIC     # get the number of cpus that Spark will use to parallelize for splitting
# MAGIC     sc = SparkContext.getOrCreate()
# MAGIC     num_cpus = sc.defaultParallelism
# MAGIC     # 4 jobs per cpu is good for the goofys file deamon throughput
# MAGIC     jobs_per_cpu = 4
# MAGIC     batches_to_process = [
# MAGIC         files_to_process[i : i + jobs_per_cpu]  # noqa: E203
# MAGIC         for i in range(0, len(files_to_process), jobs_per_cpu)
# MAGIC     ]
# MAGIC     # 4 slices per CPU works well from testing
# MAGIC     num_partitions = num_cpus * 4
# MAGIC     LOGGER.info(
# MAGIC         f"sending to parallelize {len(batches_to_process)} batches among "
# MAGIC         f"{num_partitions} slices"
# MAGIC     )
# MAGIC
# MAGIC     files_rdd = sc.parallelize(batches_to_process, numSlices=num_partitions)
# MAGIC     # Process each batch partition as soon as it's ready
# MAGIC     start = time.time()
# MAGIC     nested_new_inventory_entries = files_rdd.map(
# MAGIC         lambda file_infos_to_process: process_file_node_batch(
# MAGIC             file_infos_to_process,
# MAGIC             {
# MAGIC                 "local_directory": LOCAL_EPHEMERAL_PATH,
# MAGIC                 "target_directory": target_directory,
# MAGIC                 "existing_hash_dict": existing_hash_dict,
# MAGIC                 "ingested_file_count_dict": ingested_file_count_dict,
# MAGIC             },
# MAGIC             inventory_table_fqdn,
# MAGIC         )
# MAGIC     ).collect()
# MAGIC     
# MAGIC     new_inventory_entries = [
# MAGIC         x for local_inventory_entries in nested_new_inventory_entries 
# MAGIC         for x in local_inventory_entries]
# MAGIC
# MAGIC     LOGGER.info(f"ALL DONE! took {time.time()-global_start_time:.4f}s {(time.time()-start)/len(files_to_process):.2f}s for {len(new_inventory_entries)}")
# MAGIC     if new_inventory_entries:
# MAGIC         new_df = spark.createDataFrame(new_inventory_entries)
# MAGIC         new_df.write.format("delta").mode("append").saveAsTable(inventory_table_fqdn)
# MAGIC     
# MAGIC     
# MAGIC if __name__ == "__main__":
# MAGIC     main()
# MAGIC
