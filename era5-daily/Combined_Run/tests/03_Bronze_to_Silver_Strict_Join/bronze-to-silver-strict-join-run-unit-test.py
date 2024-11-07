# Databricks notebook source
from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# COMMAND ----------

if workspace_url != dev_workspace_url: 
    ## Skip the rest of the notebook
    dbutils.notebook.exit("Not in dev workspace. Skipping unit tests.")

else: 
    print("We are in dev workspace, proceed with the unit test")

# COMMAND ----------

# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, desc, row_number
from pyspark.databricks.sql.functions import h3_longlatash3
from delta.tables import DeltaTable
from pyspark.sql.window import Window 


import mosaic as mos
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

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

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, LongType, BinaryType, BooleanType
from datetime import datetime


class TestBronzeToSilverERA5CountryStrictAutoloader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Define the bronze ERA5 schema
        cls.bronze_era5_schema = StructType([
            StructField("time", TimestampType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("mean_t2m_c", FloatType(), True),
            StructField("max_t2m_c", FloatType(), True),
            StructField("min_t2m_c", FloatType(), True),
            StructField("sum_tp_mm", FloatType(), True),
            StructField("file_modified_in_s3", TimestampType(), True),
            StructField("source_file", StringType(), True),
            StructField("Source_File_Path", StringType(), True),
            StructField("Ingest_Timestamp", TimestampType(), True),
            StructField("data_provider", StringType(), True),
            StructField("date_created", TimestampType(), True)
        ])

        # Define the bronze ERA5 data
        cls.bronze_era5_data = [
            (
                datetime(2024, 1, 1, 0, 0),  # time
                45.0,  # latitude
                90.0,  # longitude
                12.5,  # mean_t2m_c
                15.0,  # max_t2m_c
                10.0,  # min_t2m_c
                100.5,  # sum_tp_mm
                datetime(2024, 1, 2, 0, 0),  # file_modified_in_s3
                "source_file_1",  # source_file
                "/path/to/source/file_1",  # Source_File_Path
                datetime(2024, 1, 3, 0, 0),  # Ingest_Timestamp
                "provider_1",  # data_provider
                datetime(2024, 1, 4, 0, 0)  # date_created
            )
        ]

        # Create the test DataFrame for bronze ERA5 data
        cls.bronze_era5_df = spark.createDataFrame(cls.bronze_era5_data, cls.bronze_era5_schema)

        # Define the country index schema
        cls.country_index_schema = StructType([
            StructField("grid_index", LongType(), True),
            StructField("country", StringType(), True),
            StructField("chip_is_core", BooleanType(), True),
            StructField("chip_wkb", BinaryType(), True)
        ])

        # Define the country index data
        cls.country_index_data = [
            (
                1234567890,  # grid_index
                "Country1",  # COUNTRY
                True,  # chip_is_core
                b"AAAAAAYAAAADAAAAAAMAAAABAAAAEkBJy8n8/0sjQDl5qsH83TdAScu/Ff5xVEA5eaNf/tUuQEnLuaH/qPlAOXmvrfx2B0BJy7WK/ilhQDl5xdQA3KVAScu25/9N3EA5edSX/zUQQEnLuEUAcldAOXn3DAERQ0BJy7bn/03cQDk"  # chip_wkb
            )
        ]

        # Create the test DataFrame for country index data
        cls.country_index_df = spark.createDataFrame(cls.country_index_data, cls.country_index_schema)

    @patch("pyspark.sql.SparkSession.readStream")
    @patch("pyspark.sql.SparkSession.table")
    @patch("pyspark.sql.functions.lit")
    @patch("pyspark.sql.functions.col", side_effect=lambda col_name: col(col_name))
    @patch("__main__.h3_longlatash3", side_effect=lambda lon, lat, res: lit(1234567890))  # Mock grid index
    @patch("mosaic.st_contains", side_effect=lambda geom, point: lit(True))  # Mock point-in-polygon check
    @patch("mosaic.st_point", side_effect=lambda lon, lat: lit("mock_point"))  # Mock st_point
    def test_bronze_to_silver_era5_country_strict_autoloader(self, mock_st_point, mock_st_contains, mock_h3_longlatash3, mock_col, mock_lit, mock_table, mock_readStream):
        # Mocking the DataFrame that is returned from Spark readStream and table
        mock_readStream.table.return_value = self.bronze_era5_df
        mock_table.return_value = self.country_index_df

        # Call the function under test
        result_df = bronze_to_silver_era5_country_strict_autoloader(
            bronze_era5_table="bronze_era5_table_mock",
            country_index_table="country_index_table_mock",
            lat_col="latitude",
            lon_col="longitude",
            target_resolution=5,
            join_type="left"
        )

        # Check if the transformations were applied correctly
        self.assertIsInstance(result_df, DataFrame)

        # Verify expected columns exist
        expected_columns = ["time", "latitude", "longitude", "mean_t2m_c", "max_t2m_c",
                            "min_t2m_c", "sum_tp_mm", "file_modified_in_s3", "source_file",
                            "Source_File_Path", "Ingest_Timestamp", "data_provider", "date_created",
                            "grid_index",  # Add grid_index here
                            "country_strict", "country_chip_wkb"]
        self.assertEqual(result_df.columns, expected_columns)

        # Verify the mocked functions were called
        mock_h3_longlatash3.assert_called_once()
        mock_st_contains.assert_called_once()
        mock_st_point.assert_called_once()

# Function to run the tests manually and raise SystemExit if any test fails
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestBronzeToSilverERA5CountryStrictAutoloader("test_bronze_to_silver_era5_country_strict_autoloader"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()

