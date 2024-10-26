
import unittest
from unittest.mock import patch, MagicMock, call
import os
import shutil
import tempfile
from datetime import datetime, date
from utils import copy_and_move_files_by_date_and_keep_inventory


class TestCopyAndMoveFilesByDateAndKeepInventory(unittest.TestCase):

    @patch('os.listdir')
    @patch('shutil.move')
    @patch('xarray.open_dataset')
    @patch('delta.tables.DeltaTable.forName')
    def test_copy_and_move_files_by_date_and_keep_inventory(self, mock_delta_forName, mock_open_dataset, mock_shutil_move, mock_os_listdir):
        """
        Test that files matching the date range and prefix are processed, moved,
        and renamed correctly with _v1.1 or _unknown_version, and that the inventory table is updated.
        """
        # Mocking os.listdir to return a list of files
        mock_os_listdir.return_value = [
            'reanalysis-era5-sfc-daily-1950-01-01.nc', 
            'reanalysis-era5-sfc-daily-2023-12-31.nc'
        ]
        
        # Mocking xarray.open_dataset to return a mock dataset with various date_updated formats
        mock_ds_1950 = MagicMock()
        mock_ds_1950.attrs = {'date_updated': '05/15/2024'}  # newer date
        
        mock_ds_2023 = MagicMock()
        mock_ds_2023.attrs = {'date_updated': None}  # triggers _unknown_version
        
        # Mock the open_dataset function to return the dataset based on the file name
        def mock_open_dataset_side_effect(filepath):
            print(f"Mock open_dataset called with: {filepath}")
            if '1950-01-01' in filepath:
                return mock_ds_1950
            if '2023-12-31' in filepath:
                return mock_ds_2023
            return None
        
        mock_open_dataset.side_effect = mock_open_dataset_side_effect
        
        # Mocking DeltaTable.forName() to simulate interaction with a Delta table
        mock_delta_table = MagicMock()
        mock_delta_forName.return_value = mock_delta_table
        
        # Mock the existing file DataFrame to simulate existing records in the Delta table
        mock_delta_table.toDF.return_value.filter.return_value.collect.return_value = [
            {'date_updated': date(2023, 5, 15), 'date_modified_in_s3': datetime(2023, 5, 15, 13, 57, 48)}
        ]
        
        # Mocking the spark session
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()  # Mock the DataFrame creation

        # Set up parameters for the test
        start_date = '1950-01-01'
        end_date = '2023-12-31'
        source_folder = tempfile.mkdtemp()  # Dynamically generated source folder
        target_folder = tempfile.mkdtemp()  # Dynamically generated target folder
        prefix = 'reanalysis-era5-sfc-daily-'
        date_pattern = '%Y-%m-%d'
        source_file_attr = 'source_file'

        # Create mock files in the source folder
        with open(os.path.join(source_folder, 'reanalysis-era5-sfc-daily-1950-01-01.nc'), 'w') as f:
            f.write('test file 1')
        with open(os.path.join(source_folder, 'reanalysis-era5-sfc-daily-2023-12-31.nc'), 'w') as f:
            f.write('test file 2')

        print(f"Source folder: {source_folder}")
        print(f"Target folder: {target_folder}")

        # Run the function to test with mocked spark
        copy_and_move_files_by_date_and_keep_inventory(
            spark=mock_spark,  # Pass the mocked spark session here
            start_date=start_date, 
            end_date=end_date, 
            source_folder=source_folder, 
            target_folder=target_folder, 
            prefix=prefix, 
            table_name="test_table", 
            date_pattern=date_pattern, 
            source_file_attr=source_file_attr
        )

        # Assert that the renamed file is moved to the correct location
        mock_shutil_move.assert_any_call(
            os.path.join('/tmp', 'reanalysis-era5-sfc-daily-1950-01-01_v1.1.nc'),  # Renamed file
            os.path.join(target_folder, 'reanalysis-era5-sfc-daily-1950-01-01_v1.1.nc')  # Target: renamed file
        )

        mock_shutil_move.assert_any_call(
            os.path.join('/tmp', 'reanalysis-era5-sfc-daily-2023-12-31_unknown_version.nc'),  # Renamed file
            os.path.join(target_folder, 'reanalysis-era5-sfc-daily-2023-12-31_unknown_version.nc')  # Target: renamed file
        )

        # Assert the Delta table is called for schema evolution
        self.assertTrue(mock_delta_forName.called)

        # Clean up temporary directories
        shutil.rmtree(source_folder)
        shutil.rmtree(target_folder)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
