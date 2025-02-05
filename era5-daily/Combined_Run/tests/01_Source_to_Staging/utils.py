import os
import xarray as xr
from datetime import datetime
import shutil
import netCDF4 as nc 

from delta.tables import DeltaTable
from pyspark.sql import SparkSession


from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import to_date

def copy_and_move_files_by_date_and_keep_inventory(spark, start_date, end_date, source_folder, target_folder, prefix, table_name="pilot.bronze_test.era5_inventory_table", date_pattern='%Y-%m-%d', source_file_attr='source_file'):
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
    # Define a function to check schema, cast, and handle schema evolution
    def perform_schema_evolution_with_debug(spark, df, table_name):
        # Load the existing schema from the Delta table
        delta_table = DeltaTable.forName(spark, table_name)
        existing_schema = delta_table.toDF().schema
        print("Existing Delta Table Schema:")
        for field in existing_schema:
            print(f"Field: {field.name}, Type: {field.dataType}")

        # Print the schema of the DataFrame you're trying to write
        print("DataFrame Schema:")
        for field in df.schema:
            print(f"Field: {field.name}, Type: {field.dataType}")

        # Cast the fields to match the Delta table schema if needed
        df = df.withColumn("date_updated", df["date_updated"].cast(DateType()))  # Ensure DateType consistency
        df = df.withColumn("date_modified_in_s3", df["date_modified_in_s3"].cast(TimestampType()))

        # Write the DataFrame with schema evolution enabled
        (df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")  # Enable schema evolution
         .saveAsTable(table_name))

    # Parse dates
    start_date = datetime.strptime(start_date, date_pattern)
    end_date = datetime.strptime(end_date, date_pattern)
    print(f"Processing files between {start_date} and {end_date}.")

    # List all files in the source folder that match the prefix
    all_files = [filename for filename in os.listdir(source_folder) if filename.startswith(prefix) and filename.endswith(".nc")]
    print(f"All files found: {all_files}")

    # Initialize list for files within date range
    filepaths_in_range = []

    # For each file in the list, extract date and check if it's in the range
    for filename in all_files:
        # Replace underscores with hyphens in the date part of the filename
        filename_with_hyphens = filename.replace('_', '-')
        # Extract date from filename
        date_str = filename_with_hyphens.split('-')[-3] + '-' + filename_with_hyphens.split('-')[-2] + '-' + filename_with_hyphens.split('-')[-1].split('.')[0]  # Assumes 'YYYY-MM-DD'
        file_date = datetime.strptime(date_str, date_pattern)

        # Check if the file date is within the range
        if start_date <= file_date <= end_date:
            filepath = os.path.join(source_folder, filename)
            filepaths_in_range.append(filepath)

    print(f"Files within date range: {filepaths_in_range}")

    # Define a function to process, update metadata, and move each NetCDF file
    def process_and_move_file(filepath):
        ds = xr.open_dataset(filepath)
        filename = os.path.basename(filepath)
        date_updated = ds.attrs.get('date_updated', None)

        if date_updated:
            try:
                date_updated = datetime.strptime(date_updated, "%m/%d/%Y").date()
            except ValueError:
                print(f"Invalid date format for date_updated: {date_updated}")
                date_updated = None

        temp_file_path = os.path.join('/tmp/', filename)
        ds.to_netcdf(temp_file_path)
        date_modified_in_s3 = datetime.fromtimestamp(os.path.getmtime(filepath))

        print(f"Processing file: {filename}, date_updated: {date_updated}, date_modified_in_s3: {date_modified_in_s3}")

        with nc.Dataset(temp_file_path, 'a') as dst:
            dst.setncattr('date_updated', str(date_updated) if date_updated is not None else 'null')
            dst.setncattr(source_file_attr, filename)
            dst.setncattr('date_modified_in_s3', date_modified_in_s3.isoformat())

        delta_table = DeltaTable.forName(spark, table_name)
        existing_file_df = delta_table.toDF().filter(f"source_file = '{filename}'").collect()

        if existing_file_df:
            print(f"File '{filename}' already exists in the inventory table.")
        else:
            print(f"File '{filename}' is a brand new file in the inventory table.")

        # Handle versioning
        new_version = False  # Add a flag to track whether it's a new version
        if date_updated is None:
            filename = filename.replace('.nc', '_unknown_version.nc')
            temp_file_path = temp_file_path.replace('.nc', '_unknown_version.nc')
            new_version = True
            print(f"Appending unknown version of '{filename}' to inventory.")
        else:
            if existing_file_df:
                existing_file = existing_file_df[0]
                existing_date_updated = existing_file['date_updated']
                existing_date_modified_in_s3 = existing_file['date_modified_in_s3'] if 'date_modified_in_s3' in existing_file else None

                if isinstance(existing_date_updated, str):
                    existing_date_updated = datetime.strptime(existing_date_updated, "%Y-%m-%d")

                if isinstance(existing_date_modified_in_s3, str):
                    existing_date_modified_in_s3 = datetime.fromisoformat(existing_date_modified_in_s3)

                print(f"Comparing '{filename}': date_updated ({date_updated}) vs existing_date_updated ({existing_date_updated})")
                print(f"Comparing '{filename}': date_modified_in_s3 ({date_modified_in_s3}) vs existing_date_modified_in_s3 ({existing_date_modified_in_s3})")

                if (existing_date_updated is None or date_updated > existing_date_updated) or \
                   (existing_date_modified_in_s3 and date_modified_in_s3 > existing_date_modified_in_s3):
                    filename = filename.replace('.nc', '_v1.1.nc')
                    temp_file_path = temp_file_path.replace('.nc', '_v1.1.nc')
                    new_version = True
                    print(f"Appending new version '{filename}' to inventory.")

        # Before appending, print whether it's a new version or not
        if new_version:
            print(f"File '{filename}' is a new version (either _v1.1 or _unknown).")
        else:
            print(f"File '{filename}' is NOT a new version (no _v1.1 or _unknown).")

        # Only append if it's a new version or unknown version
        if new_version or not existing_file_df:
            print(f"Appending file '{filename}' to inventory table.")
            target_file_path = os.path.join(target_folder, filename)
            shutil.move(temp_file_path, target_file_path)
            metadata = [(date_updated, filename, target_file_path, date_modified_in_s3.isoformat())]
            metadata_df = spark.createDataFrame(metadata, schema=["date_updated", "source_file", "Source_File_Path", "date_modified_in_s3"])

            metadata_df = metadata_df.withColumn("date_updated", metadata_df["date_updated"].cast(DateType()))
            metadata_df = metadata_df.withColumn("date_modified_in_s3", metadata_df["date_modified_in_s3"].cast(TimestampType()))

            perform_schema_evolution_with_debug(spark, metadata_df, table_name)
        else:
            print(f"No append for '{filename}', no version change.")

        return f"Processed and moved {filename} to {target_folder}"

    results = [process_and_move_file(filepath) for filepath in filepaths_in_range]

    for result in results:
        print(result)

    print("File processing, metadata update, and move complete.")




def check_netcdf_files(directory, expected_lon=1440, expected_lat=721):
    """
    Processes NetCDF files in a specified directory, checking for expected longitude and latitude points.

    Parameters
    ----------
    directory : str
        Path to the directory containing NetCDF files.
        
    expected_lon : int, optional, default=1440
        The expected number of longitude points in each file.
        
    expected_lat : int, optional, default=721
        The expected number of latitude points in each file.
        
    Returns
    -------
    None
        The function prints the names of files that do not meet the specified criteria.
        If all files meet the criteria, it prints a message indicating that all files are valid.
    """
    
    all_files_valid = True  # Flag to track if all files meet the criteria
    
    # Check if the directory exists
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return
    
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.nc'):  # Process only NetCDF files
            file_path = os.path.join(directory, filename)
            try:
                # Open the NetCDF file using xarray
                ds = xr.open_dataset(file_path)
                
                # Check the dimensions of the dataset using ds.sizes
                lon_points = ds.sizes.get('longitude', 0)
                lat_points = ds.sizes.get('latitude', 0)
                
                # Print the filename if it doesn't meet the criteria
                if lon_points != expected_lon or lat_points != expected_lat:
                    print(f"File {filename} has {lon_points} longitude points and {lat_points} latitude points.")
                    all_files_valid = False  # Set flag to False if any file does not meet the criteria
                
                # Close the dataset
                ds.close()
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
                all_files_valid = False  # Set flag to False if there is an error in processing

    # If all files meet the criteria, print a success message
    if all_files_valid:
        print("All files in the directory meet the longitude and latitude criteria.")




                            

           



