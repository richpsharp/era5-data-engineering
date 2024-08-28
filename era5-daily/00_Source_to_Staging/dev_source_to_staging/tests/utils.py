import os
import xarray as xr
from datetime import datetime
import shutil
import netCDF4 as nc 



def copy_and_move_files_by_date(start_date, end_date, source_folder, target_folder, prefix, date_pattern='%Y-%m-%d', source_file_attr='source_file'):
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


import os
import xarray as xr

def check_netcdf_files(directory, expected_lon=1440, expected_lat=721):
    """
    Processes NetCDF files in a specified directory, checking for expected longitude and latitude points.

    Parameters
    ----------
    directory : str
        Path to the directory containing NetCDF files.
        
    expected_lon : int, optional, default=1440
        The expected number of longitude points in each file.
        
    expected_lat : int, optional, default=721
        The expected number of latitude points in each file.
        
    Returns
    -------
    None
        The function prints the names of files that do not meet the specified criteria.
        If all files meet the criteria, it prints a message indicating that all files are valid.
    """
    
    all_files_valid = True  # Flag to track if all files meet the criteria
    
    # Check if the directory exists
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return
    
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.nc'):  # Process only NetCDF files
            file_path = os.path.join(directory, filename)
            try:
                # Open the NetCDF file using xarray
                ds = xr.open_dataset(file_path)
                
                # Check the dimensions of the dataset using ds.sizes
                lon_points = ds.sizes.get('longitude', 0)
                lat_points = ds.sizes.get('latitude', 0)
                
                # Print the filename if it doesn't meet the criteria
                if lon_points != expected_lon or lat_points != expected_lat:
                    print(f"File {filename} has {lon_points} longitude points and {lat_points} latitude points.")
                    all_files_valid = False  # Set flag to False if any file does not meet the criteria
                
                # Close the dataset
                ds.close()
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
                all_files_valid = False  # Set flag to False if there is an error in processing

    # If all files meet the criteria, print a success message
    if all_files_valid:
        print("All files in the directory meet the longitude and latitude criteria.")


