import unittest
from unittest.mock import patch, MagicMock
import os
import shutil
import tempfile
from datetime import datetime
from utils import copy_and_move_files_by_date 


class TestFilesInDateRange(unittest.TestCase):

    @patch('builtins.print')  # Mock print to avoid cluttering the test output
    @patch('os.listdir')
    @patch('shutil.move')
    @patch('xarray.open_dataset')
    def test_copy_and_move_files_by_date_files_in_date_range(self, mock_open_dataset, mock_shutil_move, mock_os_listdir, mock_print):
        """
        Test that files matching the date range and prefix are processed and moved correctly.
        """
        # Mocking os.listdir to return a list of files
        mock_os_listdir.return_value = ['reanalysis-era5-sfc-daily-1950-01-01.nc', 'reanalysis-era5-sfc-daily-2023-12-31.nc']
        
        # Mocking xarray.open_dataset to return a mock dataset
        mock_ds = MagicMock()
        mock_ds.attrs = {'date_updated': '2023-07-24'}
        mock_open_dataset.return_value = mock_ds

        # Set up parameters
        start_date = '1950-01-01'
        end_date = '2023-12-31'
        source_folder = tempfile.mkdtemp()
        target_folder = tempfile.mkdtemp()
        prefix = 'reanalysis-era5-sfc-daily-'
        date_pattern = '%Y-%m-%d'
        source_file_attr = 'source_file'

        # Create mock files in the source folder
        with open(os.path.join(source_folder, 'reanalysis-era5-sfc-daily-1950-01-01.nc'), 'w') as f:
            f.write('test file 1')
        with open(os.path.join(source_folder, 'reanalysis-era5-sfc-daily-2023-12-31.nc'), 'w') as f:
            f.write('test file 2')

        # Run the function
        copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern, source_file_attr)

        # Check that the files were processed and moved
        mock_shutil_move.assert_any_call(os.path.join('/tmp/', 'reanalysis-era5-sfc-daily-1950-01-01.nc'), os.path.join(target_folder, 'reanalysis-era5-sfc-daily-1950-01-01.nc'))
        mock_shutil_move.assert_any_call(os.path.join('/tmp/', 'reanalysis-era5-sfc-daily-2023-12-31.nc'), os.path.join(target_folder, 'reanalysis-era5-sfc-daily-2023-12-31.nc'))

        # Clean up temporary directories
        shutil.rmtree(source_folder)
        shutil.rmtree(target_folder)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
