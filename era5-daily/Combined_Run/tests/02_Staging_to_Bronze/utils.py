
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
                

  



            

                            

           



