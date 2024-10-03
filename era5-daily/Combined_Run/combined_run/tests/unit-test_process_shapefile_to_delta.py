import pytest
import pandas as pd
import geopandas as gpd
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from utils import process_shapefile_to_delta

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-testing").getOrCreate()

def test_process_shapefile_to_delta(spark, mocker):
    # Mock dependencies
    mock_gpd_read_file = mocker.patch('utils.gpd.read_file')
    mock_tqdm = mocker.patch('utils.tqdm')
    mock_os_path_getmtime = mocker.patch('utils.os.path.getmtime')
    mock_time_strftime = mocker.patch('utils.time.strftime')
    mock_pd_Timestamp_now = mocker.patch('utils.pd.Timestamp.now')

    # Create a mock GeoDataFrame
    mock_gdf = gpd.GeoDataFrame({
        'geometry': [None, None],
        'some_column': [1, 2]
    })
    mock_gdf.make_valid.return_value = mock_gdf['geometry']
    mock_gdf.to_crs.return_value = mock_gdf
    mock_gpd_read_file.return_value = mock_gdf

    # Mocking file modification time and current timestamp
    mock_os_path_getmtime.return_value = 1234567890
    mock_time_strftime.return_value = '2024-01-01'
    mock_pd_Timestamp_now.return_value = pd.Timestamp('2024-01-02')

    # Mocking tqdm progress bar
    mock_tqdm_instance = MagicMock()
    mock_tqdm.return_value.__enter__.return_value = mock_tqdm_instance

    # Call the function with test parameters, including the spark session
    process_shapefile_to_delta(
        spark=spark,
        shapefile_path='test_shapefile.shp',
        delta_table_name='test_delta_table',
        batch_size=2,
        original_crs=4326,
        target_crs=3857
    )

    # Assertions
    mock_gpd_read_file.assert_called_once_with('test_shapefile.shp')
    mock_gdf.set_crs.assert_called_once_with(epsg=4326, inplace=True)
    mock_gdf.to_crs.assert_called_once_with(epsg=3857)
    mock_spark_create_dataframe = mocker.spy(spark, 'createDataFrame')
    mock_spark_create_dataframe.assert_called()  # Verify that Spark DataFrame creation was called
    mock_tqdm_instance.update.assert_called()  # Verify progress bar update
    mock_os_path_getmtime.assert_called_once_with('test_shapefile.shp')
    mock_time_strftime.assert_called_once_with('%Y-%m-%d', mocker.ANY)
    mock_pd_Timestamp_now.assert_called_once()
    mock_spark_create_dataframe.return_value.write.mode.return_value.format.return_value.saveAsTable.assert_called_with('test_delta_table')
