# Databricks notebook source
# MAGIC %md
# MAGIC ### Function: `spark_copy_and_move_files_by_date`
# MAGIC
# MAGIC This function processes and moves NetCDF files from a source folder to a target folder based on a specified date range and file prefix. It leverages Apache Spark for distributed processing, which is particularly useful for handling large datasets efficiently.
# MAGIC
# MAGIC #### Parameters:
# MAGIC - **start_date (str):** The start of the date range for filtering files. The date format is specified by `date_pattern`.
# MAGIC - **end_date (str):** The end of the date range.
# MAGIC - **source_folder (str):** The directory containing the source files.
# MAGIC - **target_folder (str):** The destination directory for the processed files.
# MAGIC - **prefix (str):** A prefix to filter files by name.
# MAGIC - **date_pattern (str):** The format string for parsing dates in filenames (default is '%Y-%m-%d').
# MAGIC - **source_file_attr (str):** The attribute name in the NetCDF file for the source file information (default is 'source_file').
# MAGIC
# MAGIC #### Returns:
# MAGIC - None. The function outputs the process results directly, indicating successful moves and any issues encountered.
# MAGIC
# MAGIC #### Description:
# MAGIC The function initializes a Spark session and then filters and processes files that match the given criteria (prefix and date range). Each file is processed using `xarray` for data manipulation, and metadata attributes are added or updated using `netCDF4`. Files are temporarily saved and then moved to the target folder. This process is parallelized using Spark's distributed computing capabilities to enhance performance, especially with large datasets. The function concludes by outputting the results of each file processed.

# COMMAND ----------



from pyspark.sql import SparkSession
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

    # Process each file serially
    results = [process_and_move_file(filepath) for filepath in filepaths_in_range]

    # Print results
    for result in results:
        print(result)

    print("File processing and move complete.")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Documentation for `TestSparkCopyAndMoveFilesByDate` Unit Tests
# MAGIC
# MAGIC The following document describes the unit tests for the `spark_copy_and_move_files_by_date` function. These tests validate the functionality of the function, ensuring that it correctly processes and moves NetCDF files based on specified criteria.
# MAGIC
# MAGIC ## Test Class: `TestSparkCopyAndMoveFilesByDate`
# MAGIC
# MAGIC This test class contains unit tests for the `spark_copy_and_move_files_by_date` function. The tests use the `unittest` framework and mock objects to simulate the file system and the behavior of external libraries.
# MAGIC
# MAGIC ## Test Methods
# MAGIC
# MAGIC ### `test_spark_copy_and_move_files_by_date`
# MAGIC
# MAGIC This test method verifies that the `spark_copy_and_move_files_by_date` function correctly processes and moves files that match the specified date range and prefix.
# MAGIC
# MAGIC #### Mocks Used:
# MAGIC - `print`: Mocked to avoid cluttering the test output.
# MAGIC - `os.listdir`: Mocked to return a list of files that match the prefix and date range.
# MAGIC - `shutil.move`: Mocked to simulate the file move operation.
# MAGIC - `xarray.open_dataset`: Mocked to simulate opening and processing a NetCDF file.
# MAGIC
# MAGIC #### Parameters:
# MAGIC - `start_date`: '1950-01-01'
# MAGIC - `end_date`: '2023-12-31'
# MAGIC - `source_folder`: Temporary directory created for the test.
# MAGIC - `target_folder`: Temporary directory created for the test.
# MAGIC - `prefix`: 'reanalysis-era5-sfc-daily-'
# MAGIC - `date_pattern`: '%Y-%m-%d'
# MAGIC - `source_file_attr`: 'source_file'
# MAGIC
# MAGIC #### Assertions:
# MAGIC - Verifies that the correct files were processed and moved by checking calls to `shutil.move`.
# MAGIC
# MAGIC ### `test_no_files_in_date_range`
# MAGIC
# MAGIC This test method verifies that the `spark_copy_and_move_files_by_date` function correctly handles the case where no files match the specified date range.
# MAGIC
# MAGIC #### Mocks Used:
# MAGIC - `print`: Mocked to avoid cluttering the test output.
# MAGIC - `os.listdir`: Mocked to return a list of files that do not match the date range.
# MAGIC
# MAGIC #### Parameters:
# MAGIC - `start_date`: '1950-01-01'
# MAGIC - `end_date`: '2023-12-31'
# MAGIC - `source_folder`: Temporary directory created for the test.
# MAGIC - `target_folder`: Temporary directory created for the test.
# MAGIC - `prefix`: 'reanalysis-era5-sfc-daily-'
# MAGIC - `date_pattern`: '%Y-%m-%d'
# MAGIC - `source_file_attr`: 'source_file'
# MAGIC
# MAGIC #### Assertions:
# MAGIC - Verifies that the function completes without processing any files by checking the print output.
# MAGIC

# COMMAND ----------


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




# COMMAND ----------



# Run the unit test

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

