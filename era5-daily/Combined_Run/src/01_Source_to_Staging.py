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
from utils.file_utils import copy_file_with_hash
from utils.table_definition_loader import create_table
from utils.table_definition_loader import load_table_struct
import xarray as xr

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# data starts here and we'll use it to set a threshold for when the data should
# be pulled
ERA5_START_DATE = datetime.datetime(1950, 1, 1)
DELTA_MONTHS = 3  # always search at least 3 months prior


def copy_and_move_files_by_date_and_keep_inventory(
    spark,
    start_date,
    end_date,
    source_folder,
    target_folder,
    prefix,
    table_schema,
    table_name="pilot.bronze_test.era5_inventory_table",
    date_pattern="%Y-%m-%d",
    source_file_attr="source_file",
):
    """
    Process and move NetCDF files from one folder to another based on a date range and a prefix, and update an inventory Delta table.

    Parameters:
    - start_date (str): Start date in the format specified by date_pattern.
    - end_date (str): End date in the format specified by date_pattern.
    - source_folder (str): Path to the source folder containing the files.
    - target_folder (str): Path to the target folder where the files will be moved.
    - prefix (str): Prefix of the file names to consider.
    - table_name (str): The name of the Delta table for the file inventory.
    - date_pattern (str): Date pattern in the filename (default: '%Y-%m-%d').
    - source_file_attr (str): Attribute name for source file in the NetCDF metadata (default: 'source_file').

    Returns:
    - None
    """

    def validate_and_merge_schema(spark, df, table_name):
        """
        Perform schema evolution with debug information.

        Parameters:
        - spark: SparkSession object.
        - df: DataFrame to write to the Delta table.
        - table_name: Name of the Delta table.

        Returns:
        - None
        """
        # Load the existing Delta tables
        try:
            delta_table = DeltaTable.forName(spark, table_name)
            existing_schema = delta_table.toDF().schema
            LOGGER.debug("Existing Delta Table Schema:")
            for field in existing_schema:
                LOGGER.debug(f"Field: {field.name}, Type: {field.dataType}")
        except Exception as e:
            LOGGER.debug(
                f"Delta table '{table_name}' does not exist or cannot be loaded: {e}"
            )
            existing_schema = None

        # LOGGER the incoming DataFrame schema
        LOGGER.debug("Incoming DataFrame Schema:")
        for field in df.schema:
            LOGGER.debug(f"Field: {field.name}, Type: {field.dataType}")

        # Perform type casting to ensure compatibility
        df = df.withColumn("date_updated", df["date_updated"].cast(DateType()))
        df = df.withColumn(
            "date_modified_in_s3",
            df["date_modified_in_s3"].cast(TimestampType()),
        )

        # Debug DataFrame after casting
        LOGGER.debug("DataFrame after casting:")
        df.show()

        # Write to the Delta table with schema evolution enabled
        try:
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")  # Enable schema evolution
                .saveAsTable(table_name)
            )
            LOGGER.debug(
                f"Data successfully written to Delta table '{table_name}' with schema evolution enabled."
            )
        except Exception as e:
            LOGGER.debug(f"Error while writing to Delta table: {e}")

    # Parse dates
    start_date = datetime.strptime(start_date, date_pattern)
    end_date = datetime.strptime(end_date, date_pattern)
    LOGGER.debug(f"Processing files between {start_date} and {end_date}.")

    # List all files in the source folder that match the prefix
    all_files = [
        filename
        for filename in os.listdir(source_folder)
        if filename.startswith(prefix) and filename.endswith(".nc")
    ]

    # Initialize list for files within date range
    filepaths_in_range = []

    # For each file in the list, extract date and check if it's in the range
    for filename in all_files:
        # Replace underscores with hyphens in the date part of the filename
        filename_with_hyphens = filename.replace("_", "-")
        # Extract date from filename
        date_str = (
            filename_with_hyphens.split("-")[-3]
            + "-"
            + filename_with_hyphens.split("-")[-2]
            + "-"
            + filename_with_hyphens.split("-")[-1].split(".")[0]
        )  # Assumes 'YYYY-MM-DD'
        file_date = datetime.strptime(date_str, date_pattern)

        # Check if the file date is within the range
        if start_date <= file_date <= end_date:
            filepath = os.path.join(source_folder, filename)
            filepaths_in_range.append(filepath)

    LOGGER.debug(f"Files within date range: {filepaths_in_range}")

    # Define a function to process, update metadata, and move each NetCDF file
    def process_and_move_file(filepath):
        ## check if the file exists first, if not then skip it
        if not os.path.exists(filepath):
            LOGGER.debug(f"File not found: {filepath}. Skipping.")
            return f"Skipped {filepath} (file not found)."

        ## file processing if the file exists
        ds = xr.open_dataset(filepath)
        filename = os.path.basename(filepath)
        date_updated = ds.attrs.get("date_updated", None)
        date_created = ds.attrs.get(
            "date_created", None
        )  # Retrieve date created

        ## parse date in date_updated if it exists
        if date_updated:
            try:
                date_updated = datetime.strptime(
                    date_updated, "%m/%d/%Y"
                ).date()
            except ValueError:
                LOGGER.debug(
                    f"Invalid date format for date_updated: {date_updated}"
                )
                date_updated = None

        ## parse date in date_created if it exists
        if date_created:
            try:
                date_created = datetime.strptime(
                    date_created, "%m/%d/%Y"
                ).date()
            except ValueError:
                LOGGER.debug(
                    f"Invalid date format for date_created: {date_created}"
                )
                date_created = None

        LOGGER.debug(
            f"Processing file: {filename}, date_updated: {date_updated}, date_created: {date_created}"
        )

        temp_file_path = os.path.join("/tmp/", filename)
        ds.to_netcdf(temp_file_path)
        date_modified_in_s3 = datetime.fromtimestamp(os.path.getmtime(filepath))

        LOGGER.debug(
            f"Processing file: {filename}, date_updated: {date_updated}, date_modified_in_s3: {date_modified_in_s3}"
        )

        # Update metadata in the temp file
        with nc.Dataset(temp_file_path, "a") as dst:
            dst.setncattr(
                "date_updated",
                str(date_updated) if date_updated is not None else "null",
            )
            dst.setncattr(
                "date_created",
                str(date_created) if date_created is not None else "null",
            )
            dst.setncattr(source_file_attr, filename)
            dst.setncattr(
                "date_modified_in_s3", date_modified_in_s3.isoformat()
            )

        delta_table = DeltaTable.forName(spark, table_name)

        existing_file_df = (
            delta_table.toDF().filter(f"source_file = '{filename}'").collect()
        )

        if existing_file_df:
            LOGGER.debug(
                f"File '{filename}' already exists in the inventory table."
            )
        else:
            LOGGER.debug(
                f"File '{filename}' is a brand new file in the inventory table."
            )

        # Handle versioning
        new_version = False  # Add a flag to track whether it's a new version
        if date_updated is None and date_created is None:
            # If both dates are missing, label as unknown version
            filename = filename.replace(".nc", "_unknown_version.nc")
            temp_file_path = temp_file_path.replace(
                ".nc", "_unknown_version.nc"
            )
            new_version = True
            LOGGER.debug(
                f"Appending unknown version of '{filename}' to inventory."
            )

        elif date_updated is None and date_created is not None:
            # Handle files with only date_created but no date_updated
            LOGGER.debug(
                f"Processing file '{filename}' with date_created only, treating as known version."
            )

        else:
            if existing_file_df:
                existing_file = existing_file_df[0]
                existing_date_updated = existing_file["date_updated"]
                existing_date_modified_in_s3 = (
                    existing_file["date_modified_in_s3"]
                    if "date_modified_in_s3" in existing_file
                    else None
                )

                if isinstance(existing_date_updated, str):
                    existing_date_updated = datetime.strptime(
                        existing_date_updated, "%Y-%m-%d"
                    )

                if isinstance(existing_date_modified_in_s3, str):
                    existing_date_modified_in_s3 = datetime.fromisoformat(
                        existing_date_modified_in_s3
                    )

                LOGGER.debug(
                    f"Comparing '{filename}': date_updated ({date_updated}) vs existing_date_updated ({existing_date_updated})"
                )
                LOGGER.debug(
                    f"Comparing '{filename}': date_modified_in_s3 ({date_modified_in_s3}) vs existing_date_modified_in_s3 ({existing_date_modified_in_s3})"
                )

                if (
                    existing_date_updated is None
                    or date_updated > existing_date_updated
                ) or (
                    existing_date_modified_in_s3
                    and date_modified_in_s3 > existing_date_modified_in_s3
                ):
                    filename = filename.replace(".nc", "_v1.1.nc")
                    temp_file_path = temp_file_path.replace(".nc", "_v1.1.nc")
                    new_version = True
                    LOGGER.debug(
                        f"Appending new version '{filename}' to inventory."
                    )

        # Before appending, LOGGER whether it's a new version or not
        if new_version:
            LOGGER.debug(
                f"File '{filename}' is a new version (either _v1.1 or _unknown)."
            )
        else:
            LOGGER.debug(
                f"File '{filename}' is NOT a new version (no _v1.1 or _unknown)."
            )

        # Only append if it's a new version or unknown version
        if new_version or not existing_file_df:
            LOGGER.debug(f"Appending file '{filename}' to inventory table.")
            target_file_path = os.path.join(target_folder, filename)

            # Check if the temporary file exists before moving
            if os.path.exists(temp_file_path):
                try:
                    shutil.move(temp_file_path, target_file_path)
                except OSError as e:
                    if e.errno == errno.EXDEV:
                        # Handle cross-device move
                        shutil.copy2(temp_file_path, target_file_path)
                        os.remove(temp_file_path)
                    else:
                        raise
            else:
                LOGGER.debug(
                    f"Temporary file {temp_file_path} does not exist. Skipping move operation."
                )
                return f"Skipped {filename} (temporary file not found)."

            metadata = [
                (
                    date_updated,
                    filename,
                    target_file_path,
                    date_modified_in_s3,
                    date_created,
                )
            ]

            # Debug metadata
            LOGGER.debug("Metadata before creating DataFrame:")
            for item in metadata:
                LOGGER.debug(f"  {item}")
            LOGGER.debug("Metadata field types:")
            for item in metadata[0]:
                LOGGER.debug(f"  {type(item)}")

            LOGGER.debug("Extracted or defined schema:")
            for field in table_schema:
                LOGGER.debug(f"  {field.name}: {field.dataType}")

            metadata_df = spark.createDataFrame(metadata, schema=table_schema)

            # Debug DataFrame
            LOGGER.debug("DataFrame after creation:")
            metadata_df.show()
            LOGGER.debug("DataFrame schema after creation:")
            metadata_df.LOGGERSchema()

            # Debug before casting
            LOGGER.debug("DataFrame schema before casting:")
            metadata_df.LOGGERSchema()

            metadata_df = metadata_df.withColumn(
                "date_modified_in_s3",
                metadata_df["date_modified_in_s3"].cast(TimestampType()),
            )
            metadata_df = metadata_df.withColumn(
                "date_created", metadata_df["date_created"].cast(DateType())
            )

            # Debug after casting
            LOGGER.debug("DataFrame schema after casting:")
            metadata_df.LOGGERSchema()
            metadata_df.show()

            # Call the schema evolution function
            validate_and_merge_schema(spark, metadata_df, table_name)

        else:
            LOGGER.debug(f"No append for '{filename}', no version change.")

        return f"Processed and moved {filename} to {target_folder}"

    results = [
        process_and_move_file(filepath) for filepath in filepaths_in_range
    ]

    for result in results:
        LOGGER.debug(result)

    LOGGER.debug("File processing, metadata update, and move complete.")


# def main():
# delta_table_name = "`era5-daily-data`.bronze_dev.rs_era5_inventory_table"

# table_schema = StructType(
#     [
#         StructField("date_updated", DateType(), True),
#         StructField("source_file", StringType(), True),
#         StructField("Source_File_Path", StringType(), True),
#         StructField("date_modified_in_s3", TimestampType(), True),
#         StructField("date_created", DateType(), True),
#     ]
# )

# # Check if the Delta table already exists
# if spark.catalog.tableExists(delta_table_name):
#     LOGGER.debug(f"Delta table exists: {delta_table_name}")

#     # Validate or evolve the schema to include `date_created`
#     delta_table = DeltaTable.forName(spark, delta_table_name)
#     existing_schema = delta_table.toDF().schema
#     existing_fields = {field.name for field in existing_schema.fields}
#     new_fields = {field.name for field in table_schema.fields} - existing_fields

#     if new_fields:
#         LOGGER.debug(f"Adding new fields through schema evolution: {new_fields}")
#         # Append an empty DataFrame with the updated schema to trigger schema evolution
#         empty_df = spark.createDataFrame([], table_schema)
#         empty_df.write.format("delta").mode("append").option(
#             "mergeSchema", "true"
#         ).saveAsTable(delta_table_name)
#         LOGGER.debug(f"Schema evolution completed for {delta_table_name}.")
#     else:
#         LOGGER.debug("No schema evolution required; all fields are already present.")
# else:
#     LOGGER.debug(f"Delta table does not exist: {delta_table_name}")

#     # Create a new Delta table with the defined schema
#     empty_df = spark.createDataFrame([], table_schema)
#     empty_df.write.format("delta").option("mergeSchema", "true").saveAsTable(
#         delta_table_name
#     )
#     LOGGER.debug(f"Delta table created successfully: {delta_table_name}")

# elif workspace_url == staging_workspace_url:
#     # Define the Delta table name in Databricks
#     delta_table_name = "`era5-daily-data`.bronze_staging.era5_inventory_table"

#     # Define the schema (if needed for initial creation)
#     table_schema = StructType(
#         [
#             StructField("date_updated", DateType(), True),
#             StructField("source_file", StringType(), True),
#             StructField("Source_File_Path", StringType(), True),
#             StructField("date_modified_in_s3", TimestampType(), True),
#             StructField("date_created", DateType(), True),
#         ]
#     )

#     # Check if the Delta table already exists
#     if spark.catalog.tableExists(delta_table_name):
#         LOGGER.debug(f"Delta table exists: {delta_table_name}")

#         # Validate or evolve the schema to include `date_created`
#         delta_table = DeltaTable.forName(spark, delta_table_name)
#         existing_schema = delta_table.toDF().schema
#         existing_fields = {field.name for field in existing_schema.fields}
#         new_fields = {field.name for field in table_schema.fields} - existing_fields

#         if new_fields:
#             LOGGER.debug(f"Adding new fields through schema evolution: {new_fields}")
#             # Append an empty DataFrame with the updated schema to trigger schema evolution
#             empty_df = spark.createDataFrame([], table_schema)
#             empty_df.write.format("delta").mode("append").option(
#                 "mergeSchema", "true"
#             ).saveAsTable(delta_table_name)
#             LOGGER.debug(f"Schema evolution completed for {delta_table_name}.")
#         else:
#             LOGGER.debug("No schema evolution required; all fields are already present.")
#     else:
#         LOGGER.debug(f"Delta table does not exist: {delta_table_name}")

#         # Create a new Delta table with the defined schema
#         empty_df = spark.createDataFrame([], table_schema)
#         empty_df.write.format("delta").option("mergeSchema", "true").saveAsTable(
#             delta_table_name
#         )
#         LOGGER.debug(f"Delta table created successfully: {delta_table_name}")

# else:
#     # Do not run if not in the dev or staging workspace
#     LOGGER.debug("This function is not executed in this workspace.")

# if workspace_url == dev_workspace_url:
#     # If in the dev workspace, run on a small subset of the data
#     target_folder = "/Volumes/era5-daily-data/bronze_dev/era5_gwsc_staging_folder"
#     table_name = "`era5-daily-data`.bronze_dev.era5_inventory_table"

#     start_date = "2011-05-14"
#     end_date = "2011-05-17"
#     source_folder = "/Volumes/aer-processed/era5/daily_summary"
#     prefix = "reanalysis-era5-sfc-daily-"
#     date_pattern = "%Y-%m-%d"
#     source_file_attr = "source_file"

#     # Dynamically extract schema or define it
#     if spark.catalog.tableExists(table_name):
#         table_schema = DeltaTable.forName(spark, table_name).toDF().schema
#         LOGGER.debug(f"Dynamically extracted schema for table {table_name}:")
#         for field in table_schema:
#             LOGGER.debug(f"  {field.name}: {field.dataType}")
#     else:
#         LOGGER.debug(f"Table does not exist. Please create the table first")

#     # Run your function
#     copy_and_move_files_by_date_and_keep_inventory(
#         spark,
#         start_date,
#         end_date,
#         source_folder,
#         target_folder,
#         prefix,
#         table_schema,
#         table_name,
#         date_pattern,
#         source_file_attr,
#     )

#     LOGGER.debug("Function executed in the dev workspace on a small subset of the data.")

# elif workspace_url == staging_workspace_url:
#     # If in the staging workspace, run on the entire data
#     target_folder = (
#         "/Volumes/era5-daily-data/bronze_staging/era5_gwsc_staging_folder"
#     )
#     table_name = "`era5-daily-data`.bronze_staging.era5_inventory_table"

#     start_date = "2011-05-14"
#     end_date = "2011-05-17"
#     source_folder = "/Volumes/aer-processed/era5/daily_summary"
#     prefix = "reanalysis-era5-sfc-daily-"
#     date_pattern = "%Y-%m-%d"
#     source_file_attr = "source_file"

#     # Dynamically extract schema or define it
#     if spark.catalog.tableExists(table_name):
#         table_schema = DeltaTable.forName(spark, table_name).toDF().schema
#         LOGGER.debug(f"Dynamically extracted schema for table {table_name}:")
#         for field in table_schema:
#             LOGGER.debug(f"  {field.name}: {field.dataType}")
#     else:
#         LOGGER.debug(f"Table does not exist. Please create the table first")

#     # Run your function
#     copy_and_move_files_by_date_and_keep_inventory(
#         spark,
#         start_date,
#         end_date,
#         source_folder,
#         target_folder,
#         prefix,
#         table_schema,
#         table_name,
#         date_pattern,
#         source_file_attr,
#     )

#     LOGGER.debug("Function executed in the staging workspace on the entire data.")

# else:
#     # Do not run the function if not in the dev workspace
#     LOGGER.debug("This function is not executed in this workspace.")


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

    start_date = (latest_date - relativedelta(months=DELTA_MONTHS)).date()
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
        target_file_path, target_hash = copy_file_with_hash(
            source_file_path, target_directory
        )
        LOGGER.debug(
            f"copied {source_file_path} to {target_file_path} in {time.time()-start:.2f}s"
        )
        file_info = dbutils.fs.ls(source_file_path)[0]
        source_modified_at = datetime.datetime.fromtimestamp(
            file_info.modificationTime / 1000
        )
        ingested_at = datetime.datetime.now()
        new_entry = Row(
            ingested_at=ingested_at,
            source_file_path=source_file_path,
            target_file_path=target_file_path,
            source_modified_at=source_modified_at,
            data_date=file_date,
        )
        new_df = spark.createDataFrame([new_entry])
        # Append the new entry to the inventory table (Delta table)
        new_df.write.format("delta").mode("append").saveAsTable(
            inventory_table_fqdn
        )

        break

    LOGGER.info(f"ALL DONE! took {global_start_time-time.time():.2f}s")
    return
    # main()
    # copy the file
    # hash the file
    # check the index to see if it's there already
    # if not, copy and update table


if __name__ == "__main__":
    main()
