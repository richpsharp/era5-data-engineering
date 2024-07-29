######
%pip install xarray
%pip install netCDF4 h5netcdf
%pip install dask
%pip install rioxarray
%pip install tqdm 


###########
dbutils.library.restartPython() 


##########
from pyspark.sql import SparkSession
import os
import sys
import xarray as xr
from datetime import datetime
import shutil
import pandas as pd 
import netCDF4 as nc
from pyspark.sql.functions import lit 
from tqdm.auto import tqdm 

##### 
import os
import xarray as xr
from datetime import datetime
import shutil
import netCDF4 as nc
from tqdm.auto import tqdm

def spark_copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern='%Y-%m-%d', source_file_attr='source_file'):
    """
    Process and move NetCDF files from one folder to another based on a date range and a prefix.

    Parameters:
    - start_date (str): Start date in the format specified by date_pattern.
    - end_date (str): End date in the format specified by date_pattern.
    - source_folder (str): Path to the source folder containing the files.
    - target_folder (str): Path to the target folder where the files will be moved.
    - prefix (str): Prefix of the file names to consider.
    - date_pattern (str): Date pattern in the filename (default: '%Y-%m-%d').
    - source_file_attr (str): Attribute name for source file in the NetCDF metadata (default: 'source_file').

    Returns:
    - None
    """
    # Parse dates
    start_date = datetime.strptime(start_date, date_pattern)
    end_date = datetime.strptime(end_date, date_pattern)

    # List all files in the source folder that match the prefix
    all_files = [filename for filename in os.listdir(source_folder) if filename.startswith(prefix) and filename.endswith(".nc")]

    # Initialize list for files within date range
    filepaths_in_range = []

    # For each file in the list, extract date and check if it's in the range
    for filename in all_files:
        # Replace underscores with hyphens in the date part of the filename
        filename_with_hyphens = filename.replace('_', '-')
        # Extract date from filename
        date_str = filename_with_hyphens.split('-')[-3] + '-' + filename_with_hyphens.split('-')[-2] + '-' + filename_with_hyphens.split('-')[-1].split('.')[0]  # Assumes 'YYYY-MM-DD'
        file_date = datetime.strptime(date_str, date_pattern)

        # Check if the file date is within the range
        if start_date <= file_date <= end_date:
            filepath = os.path.join(source_folder, filename)
            filepaths_in_range.append(filepath)

    # Define a function to process and move each NetCDF file
    def process_and_move_file(filepath):
        # Process the file using xarray
        ds = xr.open_dataset(filepath)

        # Get the filename without the directory
        filename = os.path.basename(filepath)

        # Get the date_updated attribute from the dataset, set to null if not present
        date_updated = ds.attrs.get('date_updated', None)

        # Save the processed dataset to a temporary file in /tmp/
        temp_file_path = os.path.join('/tmp/', filename)
        ds.to_netcdf(temp_file_path)

        # Get the modification time of the original file
        date_modified_in_s3 = datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()

        # Add date_updated, source file, and date_modified_in_s3 as metadata
        with nc.Dataset(temp_file_path, 'a') as dst:
            dst.setncattr('date_updated', str(date_updated) if date_updated is not None else 'null')
            dst.setncattr(source_file_attr, filename)
            dst.setncattr('date_modified_in_s3', date_modified_in_s3)

        # Move the temporary file to the target directory
        target_file_path = os.path.join(target_folder, filename)
        shutil.move(temp_file_path, target_file_path)
        return f"Processed and moved {filename} to {target_folder}"

    # Process and move files sequentially
    results = [process_and_move_file(filepath) for filepath in filepaths_in_range]

    # Print results
    for result in results:
        print(result)

    print("File processing and move complete.")



############## 
import unittest
from unittest.mock import patch, MagicMock
import os
import shutil
import tempfile
from datetime import datetime

class TestSparkCopyAndMoveFilesByDate(unittest.TestCase):

    @patch('builtins.print')  # Mock print to avoid cluttering the test output
    @patch('os.listdir')
    @patch('shutil.move')
    @patch('xarray.open_dataset')
    def test_spark_copy_and_move_files_by_date(self, mock_open_dataset, mock_shutil_move, mock_os_listdir, mock_print):
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
        spark_copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern, source_file_attr)

        # Check that the files were processed and moved
        mock_shutil_move.assert_any_call(os.path.join('/tmp/', 'reanalysis-era5-sfc-daily-1950-01-01.nc'), os.path.join(target_folder, 'reanalysis-era5-sfc-daily-1950-01-01.nc'))
        mock_shutil_move.assert_any_call(os.path.join('/tmp/', 'reanalysis-era5-sfc-daily-2023-12-31.nc'), os.path.join(target_folder, 'reanalysis-era5-sfc-daily-2023-12-31.nc'))

        # Clean up temporary directories
        shutil.rmtree(source_folder)
        shutil.rmtree(target_folder)

    @patch('builtins.print')  # Mock print to avoid cluttering the test output
    @patch('os.listdir')
    def test_no_files_in_date_range(self, mock_os_listdir, mock_print):
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
        spark_copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern, source_file_attr)

        # Check that no files were processed and moved
        mock_print.assert_any_call("File processing and move complete.")

        # Clean up temporary directories
        shutil.rmtree(source_folder)
        shutil.rmtree(target_folder)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)


