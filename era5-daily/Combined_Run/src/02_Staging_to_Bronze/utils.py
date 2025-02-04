
import pandas as pd
import xarray as xr
import os 
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType, LongType, BinaryType
from datetime import datetime



def netcdf_to_bronze_autoloader(spark, source_file_location, output_schema, checkpoint_location,
                                streaming_query_name, data_format, table_name, schema_name,
                                catalog_name, write_mode, data_provider, reference_ds_path,
                                date_created_attr='date_created',
                                date_update_attr='date_updated',
                                interpolation_method='linear'):
    
    ### check if the reference dataset is available
    if not os.path.exists(reference_ds_path):
        print(f"Reference dataset {reference_ds_path} not found. Please check the path and try again.")
        return ## Exit the function if the file is not found

    # Load the reference dataset for interpolation
    try: 
        ref_ds = xr.open_dataset(reference_ds_path, engine='netcdf4')
    except ValueError as e:
        print(f"Error opening reference file {reference_ds_path} with netCDF4 engine: {e}")
        ref_ds = xr.open_dataset(reference_ds_path, engine='h5netcdf')

    target_lat = ref_ds.lat.values
    target_lon = ref_ds.lon.values

    # Loading the stream
    read_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")
        .load(source_file_location)
    )

    # Function to parse netCDF files, interpolate, and convert to Spark-compatible DataFrames
    def parse_netcdf(iterator, source_file_path='Source_File_Path',
                     ingest_timestamp_column='Ingest_Timestamp', data_provider=data_provider):
        for pd_df in iterator:
            for _, row in pd_df.iterrows():
                try:
                    # Specify the engine explicitly
                    xds = xr.open_dataset(row['path'],engine='netcdf4')  # Open the NetCDF file
                except ValueError as e:
                    print(f"Error opening file {row['path']} with netCDF4 engine: {e}")
                    xds = xr.open_dataset(row['path'], engine='h5netcdf')

                # Initialize empty dictionary to store interpolated variables
                interpolated_data = {}

                # Loop through each variable in the NetCDF file, interpolating to target grid
                for variable in xds.data_vars:
                    variable_data = xds[variable]
                    interpolated_data[variable] = variable_data.interp(
                        latitude=target_lat,
                        longitude=target_lon,
                        method=interpolation_method
                    )

                # Convert interpolated data to a DataFrame
                interpolated_df = pd.concat(
                    [interpolated_data[var].to_dataframe(name=var).reset_index() for var in interpolated_data],
                    axis=1
                )
                interpolated_df = interpolated_df.loc[:, ~interpolated_df.columns.duplicated()]  # Remove duplicate columns

                # Add metadata columns
                interpolated_df[source_file_path] = row['path']
                interpolated_df[ingest_timestamp_column] = pd.Timestamp.now()
                interpolated_df['data_provider'] = data_provider
                interpolated_df['source_file'] = xds.attrs.get('source_file', None)

                
                
                # Handle date_created
                file_creation_date = xds.attrs.get(date_created_attr, None)
                if file_creation_date: 
                    interpolated_df['date_created'] = pd.to_datetime(file_creation_date,format='%Y-%m-%d', errors='coerce')
                else: 
                    interpolated_df['date_created'] = pd.NaT 


                # Handle date_updated
                file_update_date = xds.attrs.get(date_update_attr, None)
                if file_update_date:
                    interpolated_df['date_updated'] = pd.to_datetime(file_update_date,format='%Y-%m-%d', errors='coerce')
                else:
                    interpolated_df['date_updated'] = pd.NaT


                # Handle file_modified_in_s3
                file_modified_in_s3 = xds.attrs.get('date_modified_in_s3', None)
                if file_modified_in_s3:
                    interpolated_df['file_modified_in_s3'] = pd.to_datetime(file_modified_in_s3,errors='coerce')
                else: 
                    interpolated_df['file_modified_in_s3'] = pd.NaT



                # Yield the DataFrame for Spark processing
                yield interpolated_df.reset_index(drop=True)

    bronze_table = f"{catalog_name}.{schema_name}.{table_name}"

    # Autoload configuration for Spark Streaming
    autoload = (
        read_stream
        .selectExpr("_metadata.file_path as path")  # Select only file paths
        .repartition(64)
        .mapInPandas(parse_netcdf, output_schema)  # Apply the parse_netcdf function to each batch
        .writeStream  # Configure the write stream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .queryName(streaming_query_name)
        .outputMode(write_mode)
        .trigger(availableNow=True)  # Runs on new files, then shuts down
        .toTable(bronze_table)
    )




            

                            

           



