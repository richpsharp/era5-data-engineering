import os
import xarray as xr
from datetime import datetime
import shutil
import netCDF4 as nc 

from delta.tables import DeltaTable
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType, LongType, BinaryType


from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import to_date



def copy_and_move_files_by_date_and_keep_inventory(spark,start_date, end_date, source_folder, target_folder, prefix, table_schema,table_name="pilot.bronze_test.era5_inventory_table", date_pattern='%Y-%m-%d', source_file_attr='source_file'):
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
      # Load the existing Delta table
      try:
          delta_table = DeltaTable.forName(spark, table_name)
          existing_schema = delta_table.toDF().schema
          print("Existing Delta Table Schema:")
          for field in existing_schema:
              print(f"Field: {field.name}, Type: {field.dataType}")
      except Exception as e:
          print(f"Delta table '{table_name}' does not exist or cannot be loaded: {e}")
          existing_schema = None

      # Print the incoming DataFrame schema
      print("Incoming DataFrame Schema:")
      for field in df.schema:
          print(f"Field: {field.name}, Type: {field.dataType}")

      # Perform type casting to ensure compatibility
      df = df.withColumn("date_updated", df["date_updated"].cast(DateType()))
      df = df.withColumn("date_modified_in_s3", df["date_modified_in_s3"].cast(TimestampType()))

      # Debug DataFrame after casting
      print("DataFrame after casting:")
      df.show()

      # Write to the Delta table with schema evolution enabled
      try:
          (df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")  # Enable schema evolution
            .saveAsTable(table_name))
          print(f"Data successfully written to Delta table '{table_name}' with schema evolution enabled.")
      except Exception as e:
          print(f"Error while writing to Delta table: {e}")

    

    # Parse dates
    start_date = datetime.strptime(start_date, date_pattern)
    end_date = datetime.strptime(end_date, date_pattern)
    print(f"Processing files between {start_date} and {end_date}.")

    # List all files in the source folder that match the prefix
    all_files = [filename for filename in os.listdir(source_folder) if filename.startswith(prefix) and filename.endswith(".nc")]
    

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
        ## check if the file exists first, if not then skip it
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}. Skipping.")
            return f"Skipped {filepath} (file not found)."


        ## file processing if the file exists
        ds = xr.open_dataset(filepath)
        filename = os.path.basename(filepath)
        date_updated = ds.attrs.get('date_updated', None)
        date_created = ds.attrs.get('date_created', None) # Retrieve date created 

        ## parse date in date_updated if it exists
        if date_updated:
            try:
                date_updated = datetime.strptime(date_updated, "%m/%d/%Y").date()
            except ValueError:
                print(f"Invalid date format for date_updated: {date_updated}")
                date_updated = None 

        ## parse date in date_created if it exists
        if date_created: 
            try: 
                date_created = datetime.strptime(date_created, "%m/%d/%Y").date()
            except ValueError:
                print(f"Invalid date format for date_created: {date_created}")
                date_created = None  

        print(f"Processing file: {filename}, date_updated: {date_updated}, date_created: {date_created}")

        temp_file_path = os.path.join('/tmp/', filename)
        ds.to_netcdf(temp_file_path)
        date_modified_in_s3 = datetime.fromtimestamp(os.path.getmtime(filepath))

        print(f"Processing file: {filename}, date_updated: {date_updated}, date_modified_in_s3: {date_modified_in_s3}")

        # Update metadata in the temp file
        with nc.Dataset(temp_file_path, 'a') as dst:
            dst.setncattr('date_updated', str(date_updated) if date_updated is not None else 'null')
            dst.setncattr('date_created', str(date_created) if date_created is not None else 'null')
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
        if date_updated is None and date_created is None:
            # If both dates are missing, label as unknown version
            filename = filename.replace('.nc', '_unknown_version.nc')
            temp_file_path = temp_file_path.replace('.nc', '_unknown_version.nc')
            new_version = True
            print(f"Appending unknown version of '{filename}' to inventory.") 

        elif date_updated is None and date_created is not None:
            # Handle files with only date_created but no date_updated
            print(f"Processing file '{filename}' with date_created only, treating as known version.")

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
                print(f"Temporary file {temp_file_path} does not exist. Skipping move operation.")
                return f"Skipped {filename} (temporary file not found)."


            
            metadata = [(date_updated,filename, target_file_path, date_modified_in_s3, date_created)]


            # Debug metadata
            print("Metadata before creating DataFrame:")
            for item in metadata:
              print(f"  {item}")
            print("Metadata field types:")
            for item in metadata[0]:
              print(f"  {type(item)}")
            

            print("Extracted or defined schema:")
            for field in table_schema:
              print(f"  {field.name}: {field.dataType}") 

            metadata_df = spark.createDataFrame(metadata, schema=table_schema) 

            # Debug DataFrame
            print("DataFrame after creation:")
            metadata_df.show()
            print("DataFrame schema after creation:")
            metadata_df.printSchema()

            # Debug before casting
            print("DataFrame schema before casting:")
            metadata_df.printSchema()


            metadata_df = metadata_df.withColumn("date_modified_in_s3", metadata_df["date_modified_in_s3"].cast(TimestampType()))
            metadata_df = metadata_df.withColumn("date_created", metadata_df["date_created"].cast(DateType()))

            # Debug after casting
            print("DataFrame schema after casting:")
            metadata_df.printSchema()
            metadata_df.show()


            # Call the schema evolution function
            validate_and_merge_schema(spark, metadata_df, table_name)

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




                            

           



