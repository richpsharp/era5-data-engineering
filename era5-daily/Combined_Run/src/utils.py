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



import os
import time
import pandas as pd
import geopandas as gpd
from tqdm.auto import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp


####################

def process_shapefile_to_delta(spark,shapefile_path, delta_table_name, source_file_path_col_name='source_file_path',
                               source_file_name_col_name='source_file_name', date_uploaded_col_name='date_uploaded',
                               date_ingested_col_name='date_ingested', batch_size=100, original_crs=None, target_crs=None):
    """
    Processes geographic shapefiles and stores the data in a Delta table, leveraging both GeoPandas and 
    PySpark capabilities to handle spatial data and perform distributed database operations. This function 
    handles various transformations and optimizations on the shapefile data.

    Parameters
    ----------
    shapefile_path : str
        The file path to the shapefile to be processed.
    delta_table_name : str
        The name of the Delta table where data will be stored.
    source_file_path_col_name : str, optional
        Column name to store the path of the source shapefile (default: 'source_file_path').
    source_file_name_col_name : str, optional
        Column name to store the name of the source file (default: 'source_file_name').
    date_uploaded_col_name : str, optional
        Column name to store the date the file was uploaded (default: 'date_uploaded').
    date_ingested_col_name : str, optional
        Column name to store the date the data was ingested into the database (default: 'date_ingested').
    batch_size : int, optional
        Number of rows in each batch to be processed (default: 100).
    original_crs : int, optional
        The original Coordinate Reference System (CRS) code if the GeoDataFrame doesn't have one.
    target_crs : int, optional
        The target CRS code to which the data should be transformed.

    Workflow
    --------
    1. Reading and Validating Geometry:
        Loads the shapefile into a GeoDataFrame and validates the geometries, ensuring they are suitable 
        for further processing.
    
    2. CRS Management:
        Sets the original CRS if it's missing and transforms the GeoDataFrame to the target CRS if specified.
    
    3. Data Preparation:
        Converts the geometries into Well-Known Text (WKT) format for compatibility and transforms the 
        GeoDataFrame into a standard pandas DataFrame. Additional metadata columns are added to track the 
        file source and ingestion timestamps.
    
    4. Data Ingestion:
        The data is chunked based on the specified `batch_size`, and each chunk is converted into a Spark 
        DataFrame. These chunks are then appended to the specified Delta table in a distributed fashion 
        using Spark's write capabilities.

    Features
    --------
    - Spatial Data Handling:
        Utilizes GeoPandas for robust geographic data manipulation.
    
    - Efficient Data Loading:
        Implements batch processing and leverages PySpark's distributed data processing to handle large 
        datasets efficiently.
    
    - Metadata Enrichment:
        Enhances data traceability by adding comprehensive metadata regarding data source and processing 
        timestamps.

    Notes
    -----
    This function is crucial for converting shapefiles into a format suitable for analysis and storage in 
    scalable data architectures, making it invaluable for geospatial data pipelines.
    """
    
    # Read a shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path)   

    # Making the geometries valid and store it as an array or vector
    gdf_valid = gdf.make_valid()    
     
    # Replacing the old geometry with valid geometry
    gdf['geometry'] = gdf_valid

    # Set CRS if GeoDataFrame doesn't have one
    if original_crs and not gdf.crs:
        gdf.set_crs(epsg=original_crs, inplace=True)

    # Transform projection if target CRS is provided
    if target_crs:
        gdf = gdf.to_crs(epsg=target_crs)

    # Convert the geometry column to WKT format, handling None geometries
    gdf['geometry_wkt'] = gdf['geometry'].apply(lambda x: x.wkt if x is not None else None)

    # Convert the GeoDataFrame to a regular pandas DataFrame
    df = pd.DataFrame(gdf)

    # Add additional columns
    df[source_file_path_col_name] = shapefile_path
    df[source_file_name_col_name] = shapefile_path.split('/')[-1]

    # Get the modification time of the file as the upload date
    mod_time = os.path.getmtime(shapefile_path)
    df[date_uploaded_col_name] = time.strftime('%Y-%m-%d', time.localtime(mod_time))

    df[date_ingested_col_name] = pd.Timestamp.now()  # Timestamp of ingestion

    # Drop the original geometry column
    df = df.drop(columns=['geometry'])

    # Print the size of the pandas DataFrame
    print(f"Size of the pandas DataFrame: {df.shape}")

    # Chunking the pandas DataFrame and converting to Spark DataFrame for each chunk
    total_chunks = (len(df) + batch_size - 1) // batch_size
    with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
        for start in range(0, len(df), batch_size):
            end = min(start + batch_size, len(df))
            chunk_df = df.iloc[start:end]
            spark_chunk_df = spark.createDataFrame(chunk_df)
            spark_chunk_df.write.mode("append").format("delta").saveAsTable(delta_table_name)
            pbar.update(1)


import pandas as pd
import xarray as xr
import os 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType, LongType, BinaryType
from datetime import datetime

def netcdf_to_bronze_autoloader(spark,source_file_location,
                     output_schema, checkpoint_location, 
                     streaming_query_name,data_format,table_name, 
                     schema_name, catalog_name,write_mode,
                     data_provider,date_created_attr='date_created'): 
    
   ## loading the stream 
    read_stream = (spark.readStream
	.format("cloudFiles") # "cloudFiles" = use Autoloader
	.option("cloudFiles.format", "BINARYFILE") # So we don't parse it until inside the foreachBatch
 	.load(source_file_location) # Volume containing files. Can use *-wildcards
) 
    

    
    ## a function to parse netcdf files and convert them into pandas and later spark dataframes
    def parse_netcdf(iterator, source_file_path='Source_File_Path',
                     ingest_timestamp_column='Ingest_Timestamp',
                     data_provider= data_provider):  # Add default arguments for metadata
        for pd_df in iterator:
            for _, row in pd_df.iterrows():
                xds = xr.open_dataset(row['path'])  # Assume row has a 'path' column

                # Check if file contains ERA5T data
                if 'expver' in xds.variables:
                    # "Experiment version": if data older than 60 days, straightforward.
                    # If newer, this flag distinguishes old and newer data.
                    pdf = xds.sel(expver=1).drop_vars(['expver']).to_dataframe()
                else:
                    pdf = xds.to_dataframe() 

                pdf.dropna(inplace=True)  # Drop rows with null values

                # Add metadata columns
                pdf[source_file_path] = row['path']
                pdf[ingest_timestamp_column] = pd.Timestamp.now()
                pdf['data_provider'] = data_provider
                pdf['source_file'] = xds.attrs.get('source_file', None)

                # Convert file_modified_in_s3 to datetime
                file_modified_in_s3 = xds.attrs.get('date_modified_in_s3', None)
                if file_modified_in_s3:
                    pdf['file_modified_in_s3'] = pd.to_datetime(file_modified_in_s3)
                else:
                    pdf['file_modified_in_s3'] = None

                # Use the specified attribute to populate date_created, set to null if not present
                file_creation_date = xds.attrs.get(date_created_attr, None)
                if file_creation_date:
                    pdf['date_created'] = pd.to_datetime(file_creation_date)
                else:
                    pdf['date_created'] = None



                yield pdf.reset_index()  # Reset index to align time with variables  


    

    bronze_table = f"{catalog_name}.{schema_name}.{table_name}"

    autoload = (read_stream
	.selectExpr("_metadata.file_path as path") # Keep only the file paths, as /Volumes/...
  	.repartition(64)
	.mapInPandas(parse_netcdf, output_schema) # Read each file and convert to Dataframe rows
	.writeStream # Writestream config must go below all the transformations
	.option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
	.queryName(streaming_query_name)
	.outputMode(write_mode)
 	.trigger(availableNow=True) # Runs on new files, then shuts down
    .toTable(bronze_table))
                

  



            

                            

           



