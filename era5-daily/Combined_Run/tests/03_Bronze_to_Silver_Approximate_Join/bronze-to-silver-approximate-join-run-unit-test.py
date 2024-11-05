# Databricks notebook source
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION ## LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC

# COMMAND ----------

#%pip install databricks-mosaic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, desc, row_number
from pyspark.databricks.sql.functions import h3_longlatash3
from delta.tables import DeltaTable
from pyspark.sql.window import Window 


#import mosaic as mos
#mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

###########
### DEFINE FUNCTION
##############

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


import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, LongType, BinaryType, BooleanType
from datetime import datetime 



# COMMAND ----------



class TestBronzeToSilverERA5CountryAutoloader(unittest.TestCase):

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
            ),
            (
                datetime(2024, 2, 1, 0, 0),  # time
                50.0,  # latitude
                85.0,  # longitude
                13.5,  # mean_t2m_c
                16.0,  # max_t2m_c
                11.0,  # min_t2m_c
                105.5,  # sum_tp_mm
                datetime(2024, 2, 2, 0, 0),  # file_modified_in_s3
                "source_file_2",  # source_file
                "/path/to/source/file_2",  # Source_File_Path
                datetime(2024, 2, 3, 0, 0),  # Ingest_Timestamp
                "provider_2",  # data_provider
                datetime(2024, 2, 4, 0, 0)  # date_created
            )
        ]

        # Create the test DataFrame for bronze ERA5 data
        cls.bronze_era5_df = spark.createDataFrame(cls.bronze_era5_data, cls.bronze_era5_schema)

        # Define the country index schema
        cls.country_index_schema = StructType([
            StructField("geom_id", LongType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("ISO_CC", StringType(), True),
            StructField("CONTINENT", StringType(), True),
            StructField("COUNTRYAFF", StringType(), True),
            StructField("LAND_TYPE", StringType(), True),
            StructField("LAND_RANK", LongType(), True),
            StructField("Shape_Leng", FloatType(), True),
            StructField("Shape_Area", FloatType(), True),
            StructField("grid_index", LongType(), True),
            StructField("chip_is_core", BooleanType(), True),
            StructField("chip_wkb", BinaryType(), True)
        ])

        # Define the country index data
        cls.country_index_data = [
            (
                1,  # geom_id
                "Country1",  # COUNTRY
                "ISO1",  # ISO_CC
                "Continent1",  # CONTINENT
                "CountryAff1",  # COUNTRYAFF
                "LandType1",  # LAND_TYPE
                1,  # LAND_RANK
                1.1,  # Shape_Leng
                1.1,  # Shape_Area
                1234567890,  # grid_index
                True,  # chip_is_core
                b"AAAAAAYAAAADAAAAAAMAAAABAAAAEkBJy8n8/0sjQDl5qsH83TdAScu/Ff5xVEA5eaNf/tUuQEnLuaH/qPlAOXmvrfx2B0BJy7WK/ilhQDl5xdQA3KVAScu25/9N3EA5edSX/zUQQEnLuEUAcldAOXn3DAERQ0BJy7bn/03cQDk"  # chip_wkb
            ),
            (
                2,  # geom_id
                "Country2",  # COUNTRY
                "ISO2",  # ISO_CC
                "Continent2",  # CONTINENT
                "CountryAff2",  # COUNTRYAFF
                "LandType2",  # LAND_TYPE
                2,  # LAND_RANK
                2.2,  # Shape_Leng
                2.2,  # Shape_Area
                987654321,  # grid_index
                False,  # chip_is_core
                b"AAAAAAYAAAACAAAAAAMAAAABAAAADkBJxY52/9OjQDmFFJf+2LFAScWX0P8OrUA5hRBgAiLlQEnF5PT+g0hAOYVcT/zUIUBJxhq4/3/0QDmEyKgAjr1AScYYYf/jWUA5hK9X/pNOQEnF3fIA0rFAOYS8AAH5jEBJxdSX/6nJQDk"  # chip_wkb
            )
        ]

        # Create the test DataFrame for country index data
        cls.country_index_df = spark.createDataFrame(cls.country_index_data, cls.country_index_schema)

    @patch("pyspark.sql.SparkSession.readStream")
    @patch("pyspark.sql.SparkSession.table")
    @patch("pyspark.sql.functions.lit")
    @patch("pyspark.sql.functions.col", side_effect=lambda col_name: col(col_name))
    @patch("__main__.h3_longlatash3", side_effect=lambda lon, lat, res: lit(1234567890))  # Mock grid index
    def test_bronze_to_silver_era5_country_approximate_autoloader(self, mock_h3_longlatash3, mock_col, mock_lit, mock_table, mock_readStream):
        # Mocking the DataFrame that is returned from Spark readStream and table
        mock_readStream.table.return_value = self.bronze_era5_df
        mock_table.return_value = self.country_index_df

        # Call the function under test
        result_df = bronze_to_silver_era5_country_approximate_autoloader(
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
        expected_columns = ["time", "latitude", "longitude", "mean_t2m_c", "max_t2m_c", "min_t2m_c", "sum_tp_mm", "file_modified_in_s3", "source_file", "Source_File_Path", "Ingest_Timestamp", "data_provider", "date_created", "grid_index", "country"]
        self.assertEqual(result_df.columns, expected_columns)

# Function to run the tests manually and raise SystemExit if any test fails
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestBronzeToSilverERA5CountryAutoloader("test_bronze_to_silver_era5_country_approximate_autoloader"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()


# COMMAND ----------

####################
### DEFINE FUNCTION
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

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------



###########
## UNIT TEST ####
#########


class TestMergeERA5WithSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up the Spark session
        cls.spark = SparkSession.builder.master("local[1]").appName("merge_era5_with_silver_test").getOrCreate()

    @patch("delta.tables.DeltaTable.forName")  # Mock DeltaTable's forName method from delta.tables directly
    def test_merge_era5_with_silver(self, mock_DeltaTable_forName):
        # Mock the DeltaTable object and its methods
        mock_delta_table = MagicMock()
        mock_DeltaTable_forName.return_value = mock_delta_table

        # Mock the DataFrame returned from the window function
        mock_changeset_df = MagicMock(spec=DataFrame)

        # Use a real WindowSpec object instead of a mock
        window_spec = Window.partitionBy("time", "latitude", "longitude").orderBy(desc("sequence_col_mock"))

        # Simulate the output of withColumn and filter operations
        mock_changeset_df.withColumn.return_value = mock_changeset_df
        mock_changeset_df.filter.return_value = mock_changeset_df
        mock_changeset_df.drop.return_value = mock_changeset_df

        # Simulate dropDuplicates
        mock_deduped_changeset_df = MagicMock(spec=DataFrame)
        mock_changeset_df.dropDuplicates.return_value = mock_deduped_changeset_df

        # Mock the merge operations
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedDelete.return_value.execute.return_value = None
        mock_merge_builder.whenNotMatchedInsertAll.return_value.execute.return_value = None

        # Define global variables used in the function
        global target_silver_table, silver_merge_keys, silver_merge_condition, sequence_col
        target_silver_table = "mock_silver_table"
        silver_merge_keys = ["time", "latitude", "longitude"]
        silver_merge_condition = "t.time = c.time AND t.latitude = c.latitude AND t.longitude = c.longitude"
        sequence_col = "sequence_col_mock"

        # Call the function under test
        merge_era5_with_silver(mock_changeset_df, batch_id=1)

        # Verify that the DeltaTable's forName method was called with the correct table
        mock_DeltaTable_forName.assert_called_with(self.spark, target_silver_table)

        # Verify that withColumn was called
        mock_changeset_df.withColumn.assert_called_once()
        
        # Verify the column name 'row_num' and ensure row_number was used
        called_args = mock_changeset_df.withColumn.call_args[0]
        self.assertEqual(called_args[0], "row_num", "Expected 'row_num' column name")
        self.assertTrue(isinstance(called_args[1], row_number().__class__), "Expected row_number() to be used in withColumn")

        # Verify that filter was called
        mock_changeset_df.filter.assert_called_once_with("row_num = 1")
        mock_changeset_df.drop.assert_called_once_with("row_num")

        # Verify that dropDuplicates was called on the DataFrame
        mock_changeset_df.dropDuplicates.assert_called_once_with(silver_merge_keys)

        # Verify that the merge operations were called correctly
        mock_delta_table.alias.return_value.merge.assert_called()
        mock_merge_builder.whenMatchedDelete.assert_called_once_with(
            f"t.{sequence_col} IS NULL OR c.{sequence_col} > t.{sequence_col}"
        )
        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()

# Function to run the tests manually and exit if failed
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestMergeERA5WithSilver("test_merge_era5_with_silver"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()

