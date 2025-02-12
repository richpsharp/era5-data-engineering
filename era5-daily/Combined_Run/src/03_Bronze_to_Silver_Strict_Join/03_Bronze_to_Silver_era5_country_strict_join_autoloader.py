# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for performing a streaming "strict" country join of ERA5 data from the bronze-tier Delta table to the silver tier. The process involves geospatial transformations with precise spatial inclusion checks and efficient updates using Databricks Delta Lake's capabilities.
# MAGIC
# MAGIC __Authors:__ Harlan Kadish | __Maintained:__ Sambadi Majumder | __Last Modified:__ 12/12/2024
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Key Components:
# MAGIC - **Join Condition Logic**:  
# MAGIC   The notebook implements a strict join where ERA5 points are matched to country data based on:
# MAGIC   - Matching `grid_index`.
# MAGIC   - Validating that ERA5 points are either strictly within a cell that is entirely in a country or within a chip of a cell that is inside the country, using `st_contains`.
# MAGIC
# MAGIC - **Update Logic**:  
# MAGIC      A straightforward MERGE statement, with the usual sequencing
# MAGIC  
# MAGIC   
# MAGIC
# MAGIC - **Data Processing Pipeline**:  
# MAGIC   The ingestion process reads streaming ERA5 data and applies geospatial transformations using H3 indexing and Databricks Mosaic functions to enrich the data with precise country information.
# MAGIC
# MAGIC - **Geospatial Enrichment**:  
# MAGIC   ERA5 data is enriched with strict country information, enabling high-precision downstream analytics while adhering to strict spatial containment logic.
# MAGIC
# MAGIC This notebook ensures the incremental movement of enriched ERA5 data to the silver tier with strict join logic and robust update mechanisms to maintain high data accuracy and consistency.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPORTANT NOTE
# MAGIC
# MAGIC **This notebook for the streaming "strict" country join differs from notebook for the streaming "approximate" country join in both the join condition and the update logic.**
# MAGIC
# MAGIC - *Join condition*: For the strict join, we check the strict inclusion of an ERA5 point in a country chip. We achieve this by first checking if the point is in a cell that is definitely inside the country (i.e. "core") or the point is in a chip of a cell that is inside the country. Because most cells are core and most chips are small, this approach out-performs a simple containment check for each point and polygon.
# MAGIC - *Update logic*:  Because the incoming "changeset" of records includes just one record for each (lat, lon, time) combination, it suffices to use a straightforward MERGE statement, with the usual sequencing.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Required Libraries
# MAGIC
# MAGIC This section installs the necessary Python libraries for handling NetCDF files and multidimensional arrays. The specific version of `numpy` (1.26.4) is required to avoid compatibility issues, and the latest version should not be used as it may cause the notebook to crash.
# MAGIC
# MAGIC - **`numpy`**: Used for handling large, multidimensional arrays and matrices.
# MAGIC - **`databricks-mosaic`**: A library for advanced geospatial operations in Databricks environments, including spatial indexing and point-in-polygon queries.

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries and Initializing Functions for Data Analysis and Geospatial Operations
# MAGIC
# MAGIC This section of the notebook imports several key libraries and modules essential for data manipulation, geospatial operations, and managing Delta tables in a PySpark environment.
# MAGIC
# MAGIC ### Imports:
# MAGIC  - **`pyspark.sql.SparkSession`**: Provides an entry point to programming with Spark with functionalities for managing sessions and Spark configurations.
# MAGIC
# MAGIC  - **`pyspark.sql.functions`**: A collection of built-in functions to perform columnar operations on DataFrames:
# MAGIC    - **`col`**: Refers to a specific column by name in a DataFrame.
# MAGIC    - **`lit`**: Creates a column with a constant value.
# MAGIC    - **`when`**: Enables conditional logic for column values.
# MAGIC    - **`desc`**: Sorts column values in descending order.
# MAGIC    - **`row_number`**: Assigns unique row numbers within a partitioned and ordered window.
# MAGIC
# MAGIC  - **`pyspark.databricks.sql.functions`**: Includes Databricks-specific SQL functions:
# MAGIC    - **`h3_longlatash3`**: Converts longitude and latitude into H3 hierarchical hexagonal spatial index values, useful for geospatial analysis.
# MAGIC
# MAGIC  - **`delta.tables.DeltaTable`**: Supports operations on Delta tables, enabling ACID transactions, time travel, and scalable metadata management in a Spark environment.
# MAGIC    - **ACID Transactions:**
# MAGIC      - **Atomicity**: Ensures that each transaction is treated as a single unit of operation. Either all changes within the transaction are applied, or none of them are, even in the event of a failure.
# MAGIC      - **Consistency**: Guarantees that the database transitions from one valid state to another, maintaining data integrity. For example, schema constraints are respected during updates.
# MAGIC      - **Isolation**: Ensures that concurrent transactions do not interfere with each other, maintaining the illusion that each transaction runs independently.
# MAGIC      - **Durability**: Ensures that once a transaction is committed, its effects are permanently recorded, even in the face of system failures.
# MAGIC    - Delta Lake leverages ACID transactions to ensure:
# MAGIC      - Reliable and correct data processing.
# MAGIC      - Prevention of partial writes or inconsistencies during updates.
# MAGIC      - Compatibility with high-concurrency workloads in big data environments.
# MAGIC
# MAGIC  - **`pyspark.sql.window.Window`**: Defines window specifications for advanced analytics on DataFrames, enabling operations like ranking, running totals, and lag/lead analysis within a specific partition or ordered dataset.
# MAGIC
# MAGIC  - **`mosaic` (imported as `mos`)**: A library providing advanced geospatial functionalities within the Databricks environment:
# MAGIC    - **`mos.enable_mosaic(spark, dbutils)`**: Initializes Mosaic, integrating it with the current Spark session and dbutils. This enables advanced geospatial queries, spatial indexing, and manipulation of complex geospatial datasets.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC For the Merge we will use the Delta Table API: https://docs.delta.io/0.4.0/api/python/index.html. You can also create temp views and use `spark.sql("MERGE INTO ...") like https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, desc, row_number
from pyspark.databricks.sql.functions import h3_longlatash3
from delta.tables import DeltaTable
from pyspark.sql.window import Window


import mosaic as mos
mos.enable_mosaic(spark, dbutils)



# COMMAND ----------

# MAGIC %md
# MAGIC **The shuffle partitions below are tuned for one year of data. We could probably use fewer for a one-day increment. Note that this setting is like a fall-back. Spark usually makes a more informed partitioning choice automatically.**
# MAGIC
# MAGIC For geospatial data, we often disable coalescePartitions to maintain smaller partitions, even if there are many of them.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <-- tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 200)                 # <-- default is 200. Tuned so that Shuffle partitions are 150-200MB

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_to_silver_era5_country_strict_autoloader Function
# MAGIC
# MAGIC This function performs a strict spatial join between ERA5 climate data and country boundary data using H3 indexing and precise point-in-polygon checks. The key steps are as follows:
# MAGIC
# MAGIC - **Input Parameters**:
# MAGIC   - `bronze_era5_table`: Name of the ERA5 bronze table containing climate data.
# MAGIC   - `country_index_table`: Table containing H3 grid index and country boundary data.
# MAGIC   - `lat_col`, `lon_col`: Latitude and longitude columns from the ERA5 table.
# MAGIC   - `target_resolution`: H3 resolution level for spatial indexing.
# MAGIC   - `join_type`: Type of join to perform (e.g., `left` join).
# MAGIC
# MAGIC - **Streaming New Records**:
# MAGIC   - The function reads new records from the ERA5 bronze table as a streaming dataframe and calculates an H3 `grid_index` for each record using the latitude and longitude columns.
# MAGIC
# MAGIC - **Country Index Preparation**:
# MAGIC   - It reads the country index table, which contains precomputed `grid_index` values and boundary information (e.g., `chip_is_core` and `chip_wkb`).
# MAGIC
# MAGIC - **Join Condition**:
# MAGIC   - The join condition first checks if the H3 `grid_index` of the ERA5 record matches that of the country boundary. It then further refines the check by ensuring the point is either inside the core of the country cell or inside the boundary chip using `st_contains`. Because most cells are core and most chips are small, this approach out-performs a simple containment check for each point and polygon.
# MAGIC
# MAGIC - **Output**:
# MAGIC   - The function returns a dataframe with ERA5 records joined with corresponding country information. The columns `country_strict` and `country_chip_wkb` hold the country and chip boundary data for each record.
# MAGIC

# COMMAND ----------

####################
### DEFINING A FUNCTION HERE TO AVOID SPARK NOT DEFINED ERROR
################


def bronze_to_silver_era5_country_strict_autoloader(bronze_era5_table,country_index_table,lat_col,
                                                     lon_col,target_resolution,join_type):  
    
    
    
    
    ## stream new records 
    bronze_era5_new = (
    spark.readStream
    .table(bronze_era5_table)
    .withColumn("grid_index",
                h3_longlatash3(col(lon_col),
                                col(lat_col), # h3_ functions support Photon; not all st_ functions do
                                lit(target_resolution)))
    .alias("era5")) 


    country_index = (
    spark.table(country_index_table)
    .alias("country_index")
    .select("grid_index", "country", "chip_is_core", "chip_wkb"))


    # The join condition requires that the cell of the point matches a cell of the country,
    # The WHERE clause requires EITHER (1) the cell is definitely inside the country, OR (2) the point is in the chip of the cell that is inside the country.
    # Testing point-inclusion for just the small chips that might be on the border is faster than checking inclusion for the whole country.
    join_condition = [
        country_index.grid_index == bronze_era5_new.grid_index, # AND...
        col("chip_is_core") | # OR ...
        mos.st_contains(
            col("chip_wkb"),
            mos.st_point( # st_contains requires longitude in [-180, 180)
                when(col(lon_col).cast('double') >= 180, col(lon_col).cast('double') - lit(360))
                    .otherwise(col(lon_col).cast('double')),
                col(lat_col).cast('double')))
        ]

    era5_changeset = (
        bronze_era5_new
        .join(country_index, join_condition, join_type)
        # Choose final columns using the dataframe aliases from above.
        .select("era5.*", "country_index.country", "country_index.chip_wkb")
        .withColumnRenamed("country", "country_strict")
        .withColumnRenamed("chip_wkb", "country_chip_wkb")
        )   
    
    return era5_changeset
    





# COMMAND ----------

# MAGIC %md
# MAGIC ### merge_era5_with_silver Function
# MAGIC
# MAGIC This function merges incoming ERA5 climate data changes with the existing Silver-tier Delta table. It ensures that only the most recent updates are applied to the Silver table, maintaining data integrity. The key steps are as follows:
# MAGIC
# MAGIC - **Input Parameters**:
# MAGIC   - `changeset_df`: The dataframe containing new ERA5 records or changes.
# MAGIC   - `batch_id`: The batch identifier for the streaming job (used in the `foreachBatch` operation). Helps with restarting the stream where it left off after a failure.
# MAGIC
# MAGIC - **Window for Latest Records**:
# MAGIC   - The function defines a window to partition the data by the `silver_merge_keys` (e.g., `time`, `latitude`, `longitude`) and `country`, ordering by the `sequence_col` in descending order. This allows it to select the most recent record for each unique combination of keys.
# MAGIC
# MAGIC - **Filter Latest Changes**:
# MAGIC   - It filters the dataframe to retain only the latest record for each partition, using the `row_number()` function to keep the record with the highest `sequence_col` value.
# MAGIC
# MAGIC - **Merge Logic**:
# MAGIC   - The function merges the deduplicated changeset into the Silver table using the `merge` function.
# MAGIC   - **When Matched**: Updates existing rows where the incoming record's `sequence_col` is greater than or equal to the current record.
# MAGIC   - **When Not Matched**: Inserts new records from the changeset into the Silver table if they do not already exist.
# MAGIC
# MAGIC - **Flexible Update**:
# MAGIC   - The use of `whenMatchedUpdateAll()` ensures that all columns are updated if a match is found, and `whenNotMatchedInsertAll()` inserts new records if no match is found.
# MAGIC

# COMMAND ----------

####################
### DEFINING A FUNCTION HERE TO AVOID SPARK NOT DEFINED ERROR
################

def merge_era5_with_silver(changeset_df, batch_id):
    # Define a window specification to partition data by the silver merge keys
    # and order rows within each partition by the sequence column in descending order.
    # This ensures we can identify the latest update for each key.
    window = Window.partitionBy(silver_merge_keys).orderBy(desc(sequence_col))

    # Add a row number column to the DataFrame based on the defined window,
    # keeping only the latest record (row_num = 1) for each key.
    changeset_latest = (
        changeset_df.withColumn("row_num", row_number().over(window))  # Add row numbers based on the window.
        .filter("row_num = 1")  # Keep only the first row for each partition (latest update).
        .drop("row_num")  # Drop the helper row_num column after filtering.
    )

    # Load the target Delta table for merging updates.
    deltaSilverTable = DeltaTable.forName(spark, target_silver_table)

    # Merge the changeset DataFrame (latest updates) into the Delta table.
    # Alias the Delta table as "t" (target) and the changeset DataFrame as "c" (changeset).
    deltaSilverTable.alias("t").merge(
        changeset_latest.alias("c"),  # Perform the merge operation using the latest changeset DataFrame.
        silver_merge_condition  # Specify the condition for matching rows between the Delta table and changeset.
    ) \
    .whenMatchedUpdateAll(  # Update existing records in the Delta table when a match is found.
        # Update the record only if:
        # - The existing sequence value in the Delta table is NULL (indicating no valid data), OR
        # - The sequence value in the incoming record is greater than the existing sequence value.
        f"t.{sequence_col} IS NULL OR c.{sequence_col} > t.{sequence_col}"
    ) \
    .whenNotMatchedInsertAll().execute()  # Execute the merge operation.



# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Execution and Table Setup
# MAGIC
# MAGIC This code block configures the necessary parameters and functions for processing ERA5 climate data, depending on the workspace environment. 
# MAGIC
# MAGIC - **Workspace URL Check**:
# MAGIC   - The script checks the current workspace's URL to see if it is the development environment (`dev_workspace_url`). If it matches, the following configurations are applied.
# MAGIC
# MAGIC - **Key Parameters**:
# MAGIC   - `bronze_era5_table`: Name of the table containing ERA5 climate data in the bronze tier.
# MAGIC   - `target_silver_table`: Name of the Silver-tier Delta table where the processed data will be stored.
# MAGIC   - `country_index_table`: Table containing the country boundary data indexed by H3 spatial resolution.
# MAGIC   - `lat_col` and `lon_col`: Columns for latitude and longitude in the data, used for spatial joins.
# MAGIC   - `target_resolution`: H3 grid resolution for spatial indexing (set to 5).
# MAGIC   - `sequence_col`: Timestamp column used to track the creation time for records.
# MAGIC   - `silver_merge_keys`: The keys used to uniquely identify and update records in the Silver table, consisting of `time`, `latitude`, and `longitude`.
# MAGIC
# MAGIC - **Silver Table Merge Condition**:
# MAGIC   - The `silver_merge_condition` string is built by joining the key columns with equality conditions, forming the merge logic that allows for proper updates during streaming.
# MAGIC
# MAGIC - **Table Schema Setup**:
# MAGIC   - The code checks if the Silver-tier Delta table exists. If it doesn't, the table is created with the defined schema, including ERA5 data columns like temperature, precipitation, and country information.
# MAGIC
# MAGIC - **Streaming Data Write**:
# MAGIC   - The `write_stream` process handles the streaming data from the ERA5 bronze tier. It writes the results to the Silver-tier table using schema evolution and merge conditions.
# MAGIC   - The `foreachBatch` operation applies the `merge_era5_with_silver()` function to each batch of incoming data, ensuring that updates are applied efficiently.
# MAGIC
# MAGIC - **Execution Logic**:
# MAGIC   - The function runs only in the development workspace. If the workspace does not match `dev_workspace_url`, the script exits without executing the function.
# MAGIC

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# Conditional logic to set the parameter values based on the workspace URL
if workspace_url == dev_workspace_url:
    # Set the parameters for the dev workspace
    bronze_era5_table = "`era5-daily-data`.bronze_dev.aer_era5_bronze_1950_to_present_dev_interpolation"
    target_silver_table = "`era5-daily-data`.silver_dev.aer_era5_silver_strict_1950_to_present_dev_interpolation_res5"
    country_index_table = "`era5-daily-data`.silver_dev.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_dev/checkpoints/era5_silver_country_strict_dev_res5"

    ### Latitude column of the bronze table
    lat_col = "latitude"

    ## Longitude column of the silver table
    lon_col = "longitude"

    ## tessellation resolution
    target_resolution = 5

    ### sequence column 
    sequence_col = "date_created"

    # These are the columns that uniquely identify a record in the Silver table so we can find it and update it with revisions.
    silver_merge_keys = ["time", lat_col, lon_col]


    # The merge operation requires a SQL-like condition to match incoming records with records in the target table. The lines below use base-Python functions to construct a merge condition string from the list of merge keys.
    # E.g. "t.time = c.time and t.latitude = c.latitude and t.longitude = c.longitude"
    silver_merge_condition = (" and ").join(
        [f"t.{column} = c.{column}"
        for column in silver_merge_keys])
    print("Merge condition:", silver_merge_condition)

    # Run the function in the dev workspace
    era5_changeset = bronze_to_silver_era5_country_strict_autoloader(
        bronze_era5_table,
        country_index_table,
        lat_col,
        lon_col,
        target_resolution=target_resolution,
        join_type="left"
    )

    ## schema of the silver table ## create it if it doesn't exist
    ## CLUSTER BY specifies Liquid Clustering columns, which arranges the data so that queries, joins, and merges on these columns are faster.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_silver_table} (
            time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            mean_t2m_c FLOAT,
            max_t2m_c FLOAT,
            min_t2m_c FLOAT,
            sum_tp_mm FLOAT,
            file_modified_in_s3 TIMESTAMP,
            source_file STRING,
            Source_File_Path STRING,
            Ingest_Timestamp TIMESTAMP,
            data_provider STRING,
            grid_index BIGINT,
            country_strict STRING,
            country_chip_wkb BINARY,
            {sequence_col} timestamp)
        USING delta
        CLUSTER BY ({",".join(silver_merge_keys)})
    """)

    # Write the stream into the table with merge and schema evolution
    write_stream = (era5_changeset
    .writeStream
    .foreachBatch(merge_era5_with_silver) # Allows complex write logic, like MERGE
    .trigger(availableNow=True) # Process all of the new records since the last run
    .option("checkpointLocation", checkpoint)
    .outputMode("update")
    .start()
    )
    write_stream.awaitTermination() # Pauses execution of the rest of the notebook until the write is done.

    print("Function executed in the dev workspace on a small subset of the data.") 

elif workspace_url == staging_workspace_url: 

    # Set the parameters for the dev workspace
    bronze_era5_table = "`era5-daily-data`.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation"
    target_silver_table = "`era5-daily-data`.silver_staging.aer_era5_silver_strict_1950_to_present_staging_interpolation_res5"
    country_index_table = "`era5-daily-data`.silver_staging.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_staging/checkpoints/era5_silver_country_strict_staging_res5"

    ### Latitude column of the bronze table
    lat_col = "latitude"

    ## Longitude column of the silver table
    lon_col = "longitude"

    ## tessellation resolution
    target_resolution = 5

    ### sequence column for finding latest records, in case they're read out-of-order
    sequence_col = "date_created"

    # These are the columns that uniquely identify a record in the Silver table so we can find it and update it with revisions.
    silver_merge_keys = ["time", lat_col, lon_col]


    # Constuct a string, like a join condition, that describes how to match
    # incoming records with existing records. MERGE requires this.
    # This is just base Python string construction, here.
    # E.g. "t.time = c.time and t.latitude = c.latitude and t.longitude = c.longitude"
    silver_merge_condition = (" and ").join(
        [f"t.{column} = c.{column}"
        for column in silver_merge_keys])
    print("Merge condition:", silver_merge_condition)

    # Run the function in the dev workspace
    era5_changeset = bronze_to_silver_era5_country_strict_autoloader(
        bronze_era5_table,
        country_index_table,
        lat_col,
        lon_col,
        target_resolution=target_resolution,
        join_type="left"
    )

    ## schema of the silver table ## create it if it doesn't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_silver_table} (
            time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            mean_t2m_c FLOAT,
            max_t2m_c FLOAT,
            min_t2m_c FLOAT,
            sum_tp_mm FLOAT,
            file_modified_in_s3 TIMESTAMP,
            source_file STRING,
            Source_File_Path STRING,
            Ingest_Timestamp TIMESTAMP,
            data_provider STRING,
            grid_index BIGINT,
            country_strict STRING,
            country_chip_wkb BINARY,
            {sequence_col} timestamp)
        USING delta
        CLUSTER BY ({",".join(silver_merge_keys)})
    """)

    # Write the stream into the table with merge and schema evolution
    # See comments on the similar function above.
    write_stream = (era5_changeset
    .writeStream
    .foreachBatch(merge_era5_with_silver)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint)
    .outputMode("update")
    .start()
    )
    write_stream.awaitTermination()

    print("Function executed in the staging workspace on the entire dataset.") 


else:
    # Do not run the function if not in the dev or staging workspace
    print("This function is not executed in this workspace.")

