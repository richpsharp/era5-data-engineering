# Databricks notebook source
# MAGIC %md
# MAGIC **This notebook for the streaming "approximate" country join differs from notebook for the streaming "strict" country join in both the join condition and the update logic.**
# MAGIC
# MAGIC - *Join condition*: For the approximate join, we only join on the grid_index, not checking strict inclusion of an ERA5 point in a country chip.
# MAGIC - *Update logic*: The updates to in this case has multiple records for each key of (latitude, longitude, time), e.g. along country boundaries, so the MERGE whenMatched... logic becomes ambiguous. In this case, we accomplish updates by deleting any matching records first, and then inserting all new or updated records.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of the Databricks Mosaic Library
# MAGIC
# MAGIC This section involves the installation of the `databricks-mosaic` library, which is specifically designed to enhance the capabilities of Databricks for handling and analyzing geospatial data. The Mosaic library provides advanced geospatial functionality, enabling users to perform sophisticated spatial operations, spatial indexing, and seamless integration with other Databricks features. By installing this library, the notebook equips itself with the tools necessary to tackle complex geospatial tasks within the Databricks environment.

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC ## %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restarting Python Environment
# MAGIC
# MAGIC This command, `dbutils.library.restartPython()`, is used to restart the Python environment within Databricks notebooks. Restarting the Python environment is a critical step after installing new libraries or making significant changes to the environment. It ensures that all installed libraries are loaded correctly and that the environment is reset, clearing any residual state from previous computations. This operation is particularly useful when libraries that affect the entire runtime environment are added or updated.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries and Initializing Mosaic for Geospatial Analysis
# MAGIC
# MAGIC This section of the notebook imports several essential libraries and modules necessary for advanced data manipulation, geospatial analysis, and handling of Delta tables within a PySpark environment.
# MAGIC
# MAGIC #### Imports:
# MAGIC - **pyspark.sql.functions**: Provides a variety of functions to manipulate columns of a DataFrame. This includes `col`, `lit`, `when`, `desc`, and `row_number` for column referencing, literal values, conditional statements, sorting, and creating a row number respectively.
# MAGIC - **pyspark.databricks.sql.functions**: Imports Databricks-specific SQL functions like `h3_longlatash3`, which converts longitude and latitude coordinates into hierarchical hexagonal spatial index (H3) values.
# MAGIC - **delta.tables.DeltaTable**: Facilitates operations on Delta tables, which allow for ACID transactions and scalable metadata handling in Spark.
# MAGIC - **pyspark.sql.window.Window**: Enables the definition of window frames for operations over rows of a DataFrame based on certain ordering or partitioning criteria.
# MAGIC
# MAGIC **MOSAIC IS PROABABLY NOT USED**
# MAGIC #### Mosaic Initialization:
# MAGIC - **mosaic (imported as mos)**: The `mosaic` module from the `databricks-mosaic` library is imported to leverage advanced geospatial functionalities within Databricks.
# MAGIC - **mos.enable_mosaic(spark, dbutils)**: This function call initializes the Mosaic library, integrating it with the current Spark session and dbutils, enabling advanced geospatial functionalities within the Databricks environment. 
# MAGIC
# MAGIC By setting up these libraries and initializing Mosaic, the notebook is well-prepared to handle complex geospatial datasets, perform spatial queries, and efficiently manage Delta tables for scalable data processing tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC For the Merge we will use the Delta Table API: https://docs.delta.io/0.4.0/api/python/index.html. You can also create temp views and use `spark.sql("MERGE INTO ...") like https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, desc, row_number
from pyspark.databricks.sql.functions import h3_longlatash3
from delta.tables import DeltaTable
from pyspark.sql.window import Window


#import mosaic as mos
#mos.enable_mosaic(spark, dbutils)



# COMMAND ----------

# MAGIC %md
# MAGIC **The shuffle partitions below are tuned for one year of data. We could probably use fewer for a one-day increment.**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <-- tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 32)                 # <-- default is 200. Tuned so that Shuffle partitions are 150-200MB

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function: `bronze_to_silver_era5_country_approximate_autoloader`
# MAGIC
# MAGIC This function is responsible for streaming new records from the ERA5 bronze table and performing an approximate spatial join with country boundary data in the silver table. The function relies on H3 grid indexing to map ERA5 data points to specific countries.
# MAGIC
# MAGIC - **Parameters:**
# MAGIC   - `bronze_era5_table`: The name of the table containing ERA5 data in the bronze layer.
# MAGIC   - `country_index_table`: The table containing country boundary data with corresponding H3 grid indexes.
# MAGIC   - `lat_col`: The column name representing latitude in the ERA5 table.
# MAGIC   - `lon_col`: The column name representing longitude in the ERA5 table.
# MAGIC   - `target_resolution`: The H3 resolution used for spatial indexing.
# MAGIC   - `join_type`: Type of join to be used when combining ERA5 data with country boundary data (e.g., left join).
# MAGIC
# MAGIC - **Key Steps:**
# MAGIC   - **Streaming New Records:**
# MAGIC     - Reads the ERA5 data from the bronze table using Spark's `readStream` function.
# MAGIC     - Creates an H3 index (`grid_index`) for each data point based on latitude and longitude, which is essential for spatially joining the data.
# MAGIC   - **Country Index Table:**
# MAGIC     - Loads the country boundary data from the silver table, retaining only the required columns (`grid_index` and `country`), and ensures there are no duplicate records.
# MAGIC   - **Join Condition:**
# MAGIC     - Defines the join condition based on the `grid_index` field, which ensures that each ERA5 data point is matched to the correct country using H3 spatial indexing.
# MAGIC   - **Final Dataset:**
# MAGIC     - Joins the ERA5 data with the country boundary data and returns a DataFrame (`era5_changeset`) that contains the ERA5 data along with the corresponding country information.
# MAGIC
# MAGIC This function efficiently streams and processes data by leveraging H3 spatial indexing for approximate country mapping.
# MAGIC

# COMMAND ----------

####################
### DEFINING A FUNCTION HERE TO AVOID SPARK NOT DEFINED ERROR
################


def bronze_to_silver_era5_country_approximate_autoloader(bronze_era5_table,country_index_table,lat_col,
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
    .select("grid_index", "country") # Keep only columns we need
    .dropDuplicates())

    # The join condition requires that the cell of the means matches a cell of the country,
    join_condition = [country_index.grid_index == bronze_era5_new.grid_index]

    era5_changeset = (
        bronze_era5_new
        .join(country_index, join_condition, join_type)
        # Choose final columns using the dataframe aliases from above.
        .select("era5.*", "country_index.country")) 
    
    return era5_changeset
    



# COMMAND ----------

# MAGIC %md
# MAGIC ### Function: `merge_era5_with_silver`
# MAGIC
# MAGIC This function handles merging updates from the ERA5 data changeset (from the bronze table) into the silver-tier Delta table. The function ensures only the most recent records are retained and older or duplicate records are removed to maintain data consistency.
# MAGIC
# MAGIC - **Parameters:**
# MAGIC   - `changeset_df`: The DataFrame containing the new changeset data from the ERA5 bronze table.
# MAGIC   - `batch_id`: The ID of the current batch being processed, used in streaming.
# MAGIC
# MAGIC - **Key Steps:**
# MAGIC   - **Initialize Delta Table:**
# MAGIC     - The function starts by loading the Delta table (silver layer) where the ERA5 data will be merged.
# MAGIC   
# MAGIC   - **Deduplication and Sequence Ordering:**
# MAGIC     - For each key, the function uses a windowed operation (`row_number()`) to order records by the sequence column (`sequence_col`), ensuring that only the latest records are retained. 
# MAGIC     - Deduplication is based on the merge keys (`silver_merge_keys`) and the "country" column.
# MAGIC     - Rows are filtered so that only the most recent record for each key is included in the final dataset.
# MAGIC
# MAGIC   - **Delete Old Records:**
# MAGIC     - The function ensures that out-of-order records (older than the latest sequence value) are deleted from the silver table to prevent outdated information from being retained.
# MAGIC     - The deletion happens only when the sequence value of the incoming record is greater than or equal to the existing record in the silver table.
# MAGIC
# MAGIC   - **Insert Latest Changes:**
# MAGIC     - After deduplication and ordering, the latest records from the changeset are merged into the silver table.
# MAGIC     - The function uses the `merge` operation with `whenNotMatchedInsertAll()` to insert new records into the silver table.
# MAGIC
# MAGIC This function ensures data consistency by only inserting the latest records while deleting outdated data to avoid duplication and inconsistencies in the silver table.
# MAGIC

# COMMAND ----------

####################
### DEFINING A FUNCTION HERE TO AVOID SPARK NOT DEFINED ERROR
################


def merge_era5_with_silver(changeset_df, batch_id):
    deltaSilverTable = DeltaTable.forName(spark, target_silver_table)
    # To ensure we apply only the latest updates in a batch,
    # for each key, we choose the record with the latest sequence value.
    # For this dedup, we must add "country" to the key, because we allow
    # two records to differ only by the country.
    # Note that null sequence values sort last with ORDER BY ... DESC
    window = Window.partitionBy(silver_merge_keys + ["country"]).orderBy(desc(sequence_col))
    changeset_latest = (
        changeset_df.withColumn("row_num", row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )   
    # Delete matching rows
    # To prevent out-of-order processing, we delete only if the sequence
    # value of the incoming record is greater than the existing record.
    deduped_changeset_df = changeset_df.dropDuplicates(silver_merge_keys)
    deltaSilverTable.alias("t").merge(
        deduped_changeset_df.alias("c"),
        silver_merge_condition) \
    .whenMatchedDelete(
        f"t.{sequence_col} IS NULL OR c.{sequence_col} > t.{sequence_col}"
        ).execute()
    # Insert changeset of latest changes. We use merge again because
    # it's more flexible than df.write.insertInto or INSERT,
    # and we can avoid inserting if records already exist.
    deltaSilverTable.alias("t").merge(
        changeset_latest.alias("c"),
        silver_merge_condition) \
    .whenNotMatchedInsertAll() \
    .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting Up Workspace Parameters and Conditional Logic
# MAGIC
# MAGIC In this section, the script determines whether it's running in the development environment by checking the workspace URL. Based on the workspace, the script sets key parameters and executes the pipeline that moves data from the bronze to silver tier. Here’s a brief breakdown:
# MAGIC
# MAGIC 1. **Workspace URL Check**:
# MAGIC    - The script retrieves the current workspace URL and compares it with the development workspace URL (`dev_workspace_url`).
# MAGIC    - If the script is running in the development workspace, the subsequent logic is executed; otherwise, the function exits without running.
# MAGIC
# MAGIC 2. **Setting Parameter Values**:
# MAGIC    - **`bronze_era5_table`**: Points to the table where ERA5 climate data is stored in the bronze tier.
# MAGIC    - **`target_silver_table`**: Defines the target Delta table in the silver tier where the processed data will be saved.
# MAGIC    - **`country_index_table`**: Defines the table that contains the H3 grid index and country boundaries.
# MAGIC    - **`lat_col` and `lon_col`**: Specify the latitude and longitude columns in the ERA5 dataset.
# MAGIC    - **`target_resolution`**: Sets the resolution for the H3 spatial grid.
# MAGIC    - **`sequence_col`**: A column that represents the creation date for managing data updates.
# MAGIC
# MAGIC 3. **Defining Merge Keys and Condition**:
# MAGIC    - The `silver_merge_keys` (time, latitude, longitude) are defined to ensure that records can be uniquely identified and merged during the data pipeline process.
# MAGIC    - The `silver_merge_condition` is constructed dynamically by joining the keys, which will be used to merge the records during updates.
# MAGIC
# MAGIC 4. **Running the Function**:
# MAGIC    - The function `bronze_to_silver_era5_country_approximate_autoloader()` is executed to perform the spatial join between ERA5 data and the country boundary data.
# MAGIC
# MAGIC 5. **Schema Creation**:
# MAGIC    - If the target silver table does not exist, the script creates it with the necessary schema (columns like time, latitude, longitude, and other metadata).
# MAGIC    - The table is clustered by the merge keys to optimize performance.
# MAGIC
# MAGIC 6. **Writing the Stream**:
# MAGIC    - The processed data is streamed into the silver table using Delta's schema evolution and merge features.
# MAGIC    - A checkpoint location is used to track the progress of the streaming data, ensuring fault tolerance in case of failure.
# MAGIC    
# MAGIC 7. **Execution Logic**:
# MAGIC    - If the script is not running in the development workspace, it prints a message and exits without executing the pipeline.
# MAGIC    - If running in the development workspace, the function is executed on a small subset of data for testing purposes.
# MAGIC
# MAGIC This section ensures that the ERA5 data is appropriately processed and moved to the silver tier, using conditional execution and workspace-specific logic to handle the data efficiently.
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
    target_silver_table = "`era5-daily-data`.silver_dev.aer_era5_silver_approximate_1950_to_present_dev_interpolation"
    country_index_table = "`era5-daily-data`.silver_dev.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_dev/checkpoints/era5_silver_country_approximate_dev"

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


    # require some help accurately documenting this
    # E.g. "t.time = c.time and t.latitude = c.latitude and t.longitude = c.longitude"
    silver_merge_condition = (" and ").join(
        [f"t.{column} = c.{column}"
        for column in silver_merge_keys]
    )
    print("Merge condition:", silver_merge_condition)

    # Run the function in the dev workspace
    era5_changeset = bronze_to_silver_era5_country_approximate_autoloader(
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
            country STRING,
            {sequence_col} timestamp)
        USING delta
        CLUSTER BY ({",".join(silver_merge_keys)})
    """)

    # Write the stream into the table with merge and schema evolution
    write_stream = (
        era5_changeset
        .writeStream
        .format("delta")
        .option("mergeSchema", "true")  # Enable schema evolution
        .option("checkpointLocation", checkpoint)
        .foreachBatch(lambda batch_df, batch_id: (
            DeltaTable.forName(spark, target_silver_table)
            .alias("t")
            .merge(
                batch_df.alias("c"),
                silver_merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        ))
        .trigger(availableNow=True)
        .start()
    )

    write_stream.awaitTermination()  # Keeps later cells from running until the stream is done

    print("Function executed in the dev workspace on a small subset of the data.") 

elif workspace_url == staging_workspace_url: 

    # Set the parameters for the staging workspace
    bronze_era5_table = "`era5-daily-data`.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation"
    target_silver_table = "`era5-daily-data`.silver_staging.aer_era5_silver_approximate_1950_to_present_staging_interpolation"
    country_index_table = "`era5-daily-data`.silver_staging.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_staging/checkpoints/era5_silver_country_approximate_staging"

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


    # require some help accurately documenting this
    # E.g. "t.time = c.time and t.latitude = c.latitude and t.longitude = c.longitude"
    silver_merge_condition = (" and ").join(
        [f"t.{column} = c.{column}"
        for column in silver_merge_keys]
    )
    print("Merge condition:", silver_merge_condition)

    # Run the function in the dev workspace
    era5_changeset = bronze_to_silver_era5_country_approximate_autoloader(
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
            country STRING,
            {sequence_col} timestamp)
        USING delta
        CLUSTER BY ({",".join(silver_merge_keys)})
    """)

    # Write the stream into the table with merge and schema evolution
    write_stream = (
        era5_changeset
        .writeStream
        .format("delta")
        .option("mergeSchema", "true")  # Enable schema evolution
        .option("checkpointLocation", checkpoint)
        .foreachBatch(lambda batch_df, batch_id: (
            DeltaTable.forName(spark, target_silver_table)
            .alias("t")
            .merge(
                batch_df.alias("c"),
                silver_merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        ))
        .trigger(availableNow=True)
        .start()
    )

    write_stream.awaitTermination()  # Keeps later cells from running until the stream is done

    print("Function executed in the staging workspace on the entire data.") 


else:
    # Do not run the function if not in the dev or staging workspace
    print("This function is not executed in this workspace.")

