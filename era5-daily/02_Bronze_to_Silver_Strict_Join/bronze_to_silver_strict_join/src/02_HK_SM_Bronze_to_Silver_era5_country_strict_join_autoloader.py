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


import mosaic as mos
mos.enable_mosaic(spark, dbutils)



# COMMAND ----------

# MAGIC %md
# MAGIC **The shuffle partitions below are tuned for one year of data. We could probably use fewer for a one-day increment.**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <-- tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 32)                 # <-- default is 200. Tuned so that Shuffle partitions are 150-200MB

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

####################
### DEFINING A FUNCTION HERE TO AVOID SPARK NOT DEFINED ERROR
################


def merge_era5_with_silver(changeset_df, batch_id):
    # To ensure we apply only the latest updates in a batch,
    # for each key, we choose the record with the latest sequence value.
    # Note that null sequence values sort last with ORDER BY ... DESC
    window = Window.partitionBy(silver_merge_keys).orderBy(desc(sequence_col))
    changeset_latest = (
        changeset_df.withColumn("row_num", row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )
    # To prevent out-of-order processing, we update only if the sequence
    # value of the incoming record is greater than the existing record.
    deltaSilverTable = DeltaTable.forName(spark, target_silver_table)    
    deltaSilverTable.alias("t").merge(
        changeset_df.alias("c"),
        silver_merge_condition) \
    .whenMatchedUpdateAll(
        f"t.{sequence_col} IS NULL OR c.{sequence_col} > t.{sequence_col}") \
    .whenNotMatchedInsertAll() \
    .execute()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Conditional logic to set the parameter values based on the workspace URL
if workspace_url == dev_workspace_url:
    # Set the parameters for the dev workspace
    bronze_era5_table = "pilot.bronze_test.aer_era5_bronze_1950_to_present_test"
    target_silver_table = "pilot.test_silver.aer_era5_silver_strict_1950_to_present_test"
    country_index_table = "pilot.test_silver.esri_worldcountryboundaries_global_silver2"
    checkpoint = "/Volumes/pilot/test_silver/checkpoints/era5_silver_country_strict_dev"

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
    write_stream = (era5_changeset
    .writeStream
    .foreachBatch(merge_era5_with_silver)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint)
    .outputMode("update")
    .start()
    )
    write_stream.awaitTermination()

    print("Function executed in the dev workspace on a small subset of the data.")
else:
    # Do not run the function if not in the dev workspace
    print("This function is not executed in this workspace.")

