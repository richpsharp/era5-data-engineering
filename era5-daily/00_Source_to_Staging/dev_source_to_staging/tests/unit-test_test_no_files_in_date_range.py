import unittest
from unittest.mock import patch, MagicMock
import os
import shutil
import tempfile
from datetime import datetime
from utils import copy_and_move_files_by_date 


class TestNoFilesInDateRange(unittest.TestCase):

    @patch('builtins.print')  # Mock print to avoid cluttering the test output
    @patch('os.listdir')
    def test_copy_and_move_files_by_date_no_files_in_date_range(self, mock_os_listdir, mock_print):
        """
        Test that no files are processed when none match the date range.
        """
        # Mocking os.listdir to return a list of files outside the date range
        mock_os_listdir.return_value = ['reanalysis-era5-sfc-daily-1940-01-01.nc', 'reanalysis-era5-sfc-daily-1945-12-31.nc']

        # Set up parameters
        start_date = '1950-01-01'
        end_date = '2023-12-31'
        source_folder = tempfile.mkdtemp()
        target_folder = tempfile.mkdtemp()
        prefix = 'reanalysis-era5-sfc-daily-'
        date_pattern = '%Y-%m-%d'
        source_file_attr = 'source_file'

        # Run the function
        copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern, source_file_attr)

        # Check that no files were processed and moved
        mock_print.assert_any_call("File processing and move complete.")

        # Clean up temporary directories
        shutil.rmtree(source_folder)
        shutil.rmtree(target_folder)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
