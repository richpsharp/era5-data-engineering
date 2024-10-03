import pytest
from unittest import mock
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
import xarray as xr
from utils import netcdf_to_bronze_autoloader  # Import the function from the correct module

# Sample Spark session for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-testing").getOrCreate()

# Test function for the main logic
def test_netcdf_to_bronze_autoloader(spark, mocker):
    try:
        # Mock input parameters
        source_file_location = "mock_source_file_location"
        output_schema = StructType([
            StructField("column1", StringType(), True),
            StructField("column2", TimestampType(), True),
            # Add other fields as per your schema
        ])
        checkpoint_location = "mock_checkpoint_location"
        streaming_query_name = "mock_streaming_query_name"
        data_format = "BINARYFILE"
        table_name = "mock_table"
        schema_name = "mock_schema"
        catalog_name = "mock_catalog"
        write_mode = "append"
        data_provider = "mock_provider"
        date_created_attr = "date_created"
        
        # Mock Spark DataFrame and readStream method
        mock_read_stream = mocker.patch.object(spark, 'readStream', autospec=True)
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.load.return_value = MagicMock()

        # Mock Pandas DataFrame and xarray
        mock_xr_open_dataset = mocker.patch('xarray.open_dataset')
        mock_xr_open_dataset.return_value = MagicMock()
        
        mocker.patch('pandas.Timestamp.now', return_value=pd.Timestamp("2023-01-01"))

        # Call the function
        netcdf_to_bronze_autoloader(
            spark,
            source_file_location=source_file_location,
            output_schema=output_schema,
            checkpoint_location=checkpoint_location,
            streaming_query_name=streaming_query_name,
            data_format=data_format,
            table_name=table_name,
            schema_name=schema_name,
            catalog_name=catalog_name,
            write_mode=write_mode,
            data_provider=data_provider,
            date_created_attr=date_created_attr
        )

        # Assertions: check if the readStream and writeStream were called with the correct parameters
        mock_read_stream.format.assert_called_with("cloudFiles")
        mock_read_stream.option.assert_called_with("cloudFiles.format", "BINARYFILE")
        mock_read_stream.load.assert_called_with(source_file_location)

        # Verify that the xarray was called to open the dataset
        mock_xr_open_dataset.assert_called()

        # Print success message
        print("Test `netcdf_to_bronze_autoloader` ran successfully.")

    except Exception as e:
        print(f"Test `netcdf_to_bronze_autoloader` failed: {str(e)}")
        raise


# Test the parse_netcdf function separately
def test_parse_netcdf():
    try:
        # Sample data that mimics the expected input to the parse_netcdf function
        sample_pd_df = pd.DataFrame({
            'path': ['mock_path_1', 'mock_path_2'],
            'file_modified_in_s3': [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02")]
        })

        # Mock xarray dataset
        mock_xds = MagicMock()
        mock_xds.attrs = {
            'date_modified_in_s3': '2023-01-01',
            'source_file': 'mock_source_file',
            'expver': 1
        }
        mock_xds.to_dataframe.return_value = pd.DataFrame({
            'variable1': [1, 2, 3],
            'variable2': [4, 5, 6]
        })

        with mock.patch('xarray.open_dataset', return_value=mock_xds):
            # Assuming parse_netcdf is a nested function in the main function and can't be directly accessed
            # You would need to patch the internal method or simulate a call to it
            parse_netcdf = netcdf_to_bronze_autoloader.__globals__['parse_netcdf']
            result_generator = parse_netcdf([sample_pd_df.iterrows()])
            result_df = next(result_generator)
            
            # Assert that the resulting DataFrame has the expected structure and content
            assert 'path' in result_df.columns
            assert 'file_modified_in_s3' in result_df.columns
            assert 'date_created' in result_df.columns

            # Print success message
            print("Test `parse_netcdf` ran successfully.")

    except Exception as e:
        print(f"Test `parse_netcdf` failed: {str(e)}")
        raise
