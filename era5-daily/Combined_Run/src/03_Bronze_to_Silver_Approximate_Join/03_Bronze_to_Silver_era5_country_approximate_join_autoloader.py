# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook Overview
# MAGIC
# MAGIC This notebook is responsible for performing a streaming "approximate" country join of ERA5 data from the bronze-tier Delta table to the silver tier. The process involves geospatial transformations and efficient updates using Databricks Delta Lake's capabilities.
# MAGIC
# MAGIC __Authors:__ Harlan Kadish | __Maintained:__ Sambadi Majumder | __Last Modified:__ 12/12/2024
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Key Components:
# MAGIC - **Join Condition Logic**:  
# MAGIC   The notebook implements an approximate join where ERA5 points are joined to country data based only on matching `grid_index`. Unlike strict joins, it does not verify point inclusion within a country's boundaries.
# MAGIC
# MAGIC - **Update Logic**:  
# MAGIC   Updates to the silver-tier table handle potential ambiguities along boundaries by deleting existing records for matching keys and reinserting updated records. This ensures consistency and prevents duplicates.
# MAGIC
# MAGIC - **Data Processing Pipeline**:  
# MAGIC   The ingestion process reads streaming ERA5 data and applies geospatial transformations using H3 indexing and Databricks Mosaic functions.
# MAGIC
# MAGIC - **Geospatial Enrichment**:  
# MAGIC   ERA5 data is enriched with approximate country information, enabling downstream analytics while maintaining efficiency in processing.
# MAGIC
# MAGIC This notebook facilitates the movement of enriched ERA5 data to the silver tier while applying approximate join logic and ensuring data consistency through robust update mechanisms.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #IMPORTANT NOTE
# MAGIC
# MAGIC **This notebook for the streaming "approximate" country join differs from notebook for the streaming "strict" country join in both the join condition and the update logic.**
# MAGIC
# MAGIC - *Join condition*: For the approximate join, we only join on the grid_index, not checking strict inclusion of an ERA5 point in a country chip.
# MAGIC - *Update logic*: The updates to in this case has multiple records for each key of (latitude, longitude, time), e.g. along country boundaries, so the MERGE whenMatched... logic becomes ambiguous. In this case, we accomplish updates by deleting any matching records first, and then inserting all new or updated records.

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
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC

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
# MAGIC  This section of the notebook imports several key libraries and modules essential for data manipulation, 
# MAGIC  geospatial operations, and managing Delta tables in a PySpark environment.
# MAGIC
# MAGIC #### Imports:
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

# COMMAND ----------

# MAGIC %md
# MAGIC For the Merge we will use the Delta Table API: https://docs.delta.io/0.4.0/api/python/index.html. You can also create temp views and use `spark.sql("MERGE INTO ...") like https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, desc, row_number
from pyspark.databricks.sql.functions import h3_longlatash3
from delta.tables import DeltaTable
from pyspark.sql.window import Window



# COMMAND ----------

# MAGIC %md
# MAGIC **The shuffle partitions below are tuned for one year of data. We could probably use fewer for a one-day increment. Note that this setting is like a fall-back. Spark usually makes a more informed partitioning choice automatically.**
# MAGIC
# MAGIC For geospatial data, we often disable coalescePartitions to maintain smaller partitions, even if there are many of them.

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
    .dropDuplicates()) # A cell ID may appear in multiple chips

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
# MAGIC This function handles merging updates from the ERA5 data changeset (from the bronze table) into the silver-tier Delta table. Recall that this "approximate country join" silver table is designed such that each ERA5 space-time point (lat, lon, time) appears once _for each country that touches its H3 cell_. **That is, a single point can appear exactly as many times as the number of countries that neighbor it.**
# MAGIC
# MAGIC #### Multiplicity in the changeset
# MAGIC Indeed, the changeset, at this stage of the pipeline, includes climate measurements at a space-time point (henceforth simply "point"), but for the approximate country assignment pipeline, we assign each point to every country that **touches the point's H3 cell**. As a result, an incoming point from ERA5 can yield multiple records in the changeset, one for each approximately-relevant country. Put another way, the "changeset" of incoming records may include multiple records for each distinct combination of the typical merge key (lat, lon, time) from the "strict" pipeline.
# MAGIC
# MAGIC #### Non-traditional merge
# MAGIC Usually, the next step is to use the Delta Lake MERGE statement to modify the target silver table: update existing records (i.e. from revisions to previous data) and insert new records (new data); this process is called an "upsert."
# MAGIC
# MAGIC Now, the Delta Lake MERGE function, used in isolation, does not allow changesets with multiple records per key. Hence for this approximate pipeline, we use an unusual multistep process. Insead of performing "upserts" , we do the following:
# MAGIC 1. Delete any records in the target table with with the same point (lat, lon, time) as a record in the incoming changeset.
# MAGIC 2. Insert the records from the changeset that don't exist in the target table, hence updating existing records or otherwise inserting new ones.
# MAGIC Note that this two-step process does not occur in an atomic transaction: it's possible for step (1) to succeed but step (2) to fail for some reason. Nevertheless, the steps are idempotent. On rerun, step (1) can only delete records that remain from the first run, if any, and step (2) will only insert records for points that don't already exist.
# MAGIC
# MAGIC #### Details
# MAGIC
# MAGIC - **Parameters:**
# MAGIC   - `changeset_df`: The DataFrame containing the new changeset data from the ERA5 bronze table.
# MAGIC   - `batch_id`: The ID of the current batch being processed, used in streaming.
# MAGIC
# MAGIC - **Key Steps:**
# MAGIC   - **Initialize Delta Table:**
# MAGIC     - The function starts by creating a the Delta table object (silver layer) representing the table to which the ERA5 data will be merged.
# MAGIC   
# MAGIC   - **Choose latest records in changeset:**
# MAGIC     - For each key, the function uses a windowed operation (`row_number()`) to order records by the sequence column (`sequence_col`), ensuring that only the latest records are retained. 
# MAGIC     - Deduplication is based on the merge keys (`silver_merge_keys`) and the "country" column, because the same data point may be approximately associated with multiple countries.
# MAGIC     - Rows are filtered so that only the most recent record for each key+country is included in the final dataset.
# MAGIC
# MAGIC   - **Delete Old Records:**
# MAGIC     - The statement deletes records from the target silver table whose space-time point is included in the incoming changeset. In that case, the effect of this step plus the following insert step is to perform an update.
# MAGIC     - The deletion happens only when the sequence value of the incoming record is greater than or equal to the existing record in the silver table, to ensure the changes being made include the latest data.
# MAGIC     - The deletion removes _all_ records for an inconing point, regardless of country assignment, so that the multiple incoming recoreds (one per neighboring country) can entirely replace them.
# MAGIC
# MAGIC   - **Insert Latest Changes:**
# MAGIC     - After ordering and deleting matching records, the latest records from the changeset are merged into the silver table with `whenNotMatchedInsertAll()` to insert new records only.
# MAGIC
# MAGIC #### When country boundaries change:
# MAGIC - This overal ERA5 pipeline assumes country boundaries change slowly and the dataset of countriy boundaries will be updated very rarely. It also assumes that researchers want the climate history of moden countries, not their historical boundaries.
# MAGIC - Note that countries are assigned to ERA5 points in the stream, before the points are merged with the target table. This means that if country boundaries change, only incoming records will use the latest country data. That is, only records with the most recent "time" values or records with revisions will have updated countries.
# MAGIC - Therefore, when country boundaries change, these steps will update the country assignments in the silver tables:
# MAGIC   1. For the strict silver table, join the entire table with strict-join logic (grid_idex + st_contains()) to the country table and take the latest country names. Overwrite the existing strict silver table with this new result.
# MAGIC   2. For the approximate join, join the silver table with the country table only on grid_index, and overwrite the existing approximate silver table.
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
    deduped_changeset_df = changeset_latest.dropDuplicates(silver_merge_keys)
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
    target_silver_table = "`era5-daily-data`.silver_dev.aer_era5_silver_approximate_1950_to_present_dev_interpolation_res5"
    country_index_table = "`era5-daily-data`.silver_dev.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_dev/checkpoints/era5_silver_country_approximate_dev_res5"

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


    # Use base Python methods to construct a string that describes the record matching between the incoming records and the records in the target table. The Delta Lake MERGE statement requires the matching condition be a string, not e.g. a list of column names.
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
    target_silver_table = "`era5-daily-data`.silver_staging.aer_era5_silver_approximate_1950_to_present_staging_interpolation_res5"
    country_index_table = "`era5-daily-data`.silver_staging.esri_worldcountryboundaries_global_silver"
    checkpoint = "/Volumes/era5-daily-data/silver_staging/checkpoints/era5_silver_country_approximate_staging_res5"

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

    ## cluster keys maybe country (query), time(query), grid_indices (help with joins with cmip6)
    ## cluster_keys = ["country", time, grid_indices]


    # Use base Python methods to construct a string that describes the record matching between the incoming records and the records in the target table. The Delta Lake MERGE statement requires the matching condition be a string, not e.g. a list of column names.
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

