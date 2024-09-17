import unittest
from unittest.mock import patch, MagicMock
from utils import download_cmip6_netcdf_daily  # Only import the function we are testing

class TestDownloadCMIP6NetCDFDaily(unittest.TestCase):
    
    @patch('s3fs.S3FileSystem')  # Mock s3fs's S3FileSystem
    @patch('utils.download_file')  # Mock download_file from utils
    @patch('os.makedirs')  # Mock os.makedirs directly
    def test_download_cmip6_netcdf_daily(self, mock_makedirs, mock_download_file, mock_s3fs):
        start_year = 2020
        end_year = 2021
        model = 'GISS-E2-1-G'
        scenario = 'ssp585'
        variables = ['tasmax', 'tasmin']
        output_folder = '/tmp/output'
        
        mock_s3fs.return_value = MagicMock()  # Mock the S3 filesystem object
        mock_download_file.return_value = None  # Simulate successful download
        
        # Call the function being tested
        download_cmip6_netcdf_daily(start_year, end_year, model, scenario, variables, output_folder)
        
        # Verify that directories are created
        mock_makedirs.assert_called()
        
        # Verify that the download_file function was called
        self.assertTrue(mock_download_file.called)
        
        # Check that the S3 filesystem was initialized
        self.assertTrue(mock_s3fs.called)

if __name__ == '__main__':
    unittest.main()


