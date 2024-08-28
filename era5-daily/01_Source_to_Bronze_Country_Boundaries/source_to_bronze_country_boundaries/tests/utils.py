
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
