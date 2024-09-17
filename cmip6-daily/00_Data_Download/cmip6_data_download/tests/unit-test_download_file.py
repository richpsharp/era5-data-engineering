import unittest
from unittest.mock import patch, MagicMock
from utils import download_file

class TestDownloadFile(unittest.TestCase):

    @patch('os.path.exists')  # Mock os.path.exists to simulate file existence
    @patch('builtins.open', new_callable=unittest.mock.mock_open)  # Mock the open function for local file writing
    def test_download_file_success(self, mock_open, mock_exists):
        # Simulate that the file does not exist
        mock_exists.return_value = False

        # Create a mock S3 file system object
        mock_fs = MagicMock()
        
        # Mock the content returned from S3
        mock_fs.open.return_value.__enter__.return_value.read.return_value = b'file content'
        
        # Test parameters
        file_path = 's3://bucket/file.nc'
        output_path = '/tmp/file.nc'
        
        # Call the function being tested
        download_file(file_path, mock_fs, output_path)
        
        # Assert that os.path.exists was called with the correct output path
        mock_exists.assert_called_with(output_path)
        
        # Assert that local file was opened for writing
        mock_open.assert_called_with(output_path, 'wb')
        
        # Assert that the S3 file was opened for reading
        mock_fs.open.assert_called_with(file_path, mode='rb')
        
        # Assert that the file content was written
        mock_open().write.assert_called_with(b'file content')

    @patch('os.path.exists')  # Mock os.path.exists to simulate file existence
    def test_download_file_when_file_exists(self, mock_exists):
        # Simulate that the file already exists
        mock_exists.return_value = True

        # Create a mock S3 file system object
        mock_fs = MagicMock()
        
        # Test parameters
        file_path = 's3://bucket/file.nc'
        output_path = '/tmp/file.nc'
        
        # Call the function being tested
        with patch('builtins.print') as mock_print:
            download_file(file_path, mock_fs, output_path)
            
            # Assert that the file was not downloaded
            mock_fs.open.assert_not_called()
            
            # Assert that the correct message was printed
            mock_print.assert_called_with(f"File {output_path} already exists. Skipping download.")

    @patch('os.path.exists')  # Mock os.path.exists to simulate file existence
    @patch('builtins.print')  # Mock print to check for error messages
    def test_download_file_failure(self, mock_print, mock_exists):
        # Simulate that the file does not exist
        mock_exists.return_value = False

        # Create a mock S3 file system object
        mock_fs = MagicMock()
        
        # Mock the S3 open call to raise an exception
        mock_fs.open.side_effect = Exception("S3 read error")
        
        # Test parameters
        file_path = 's3://bucket/file.nc'
        output_path = '/tmp/file.nc'
        
        # Call the function being tested
        download_file(file_path, mock_fs, output_path)
        
        # Assert that the exception is printed
        mock_print.assert_called_with(f"Failed to download file {file_path}: S3 read error")

if __name__ == '__main__':
    unittest.main()

