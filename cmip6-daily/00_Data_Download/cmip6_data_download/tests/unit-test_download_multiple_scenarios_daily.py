import unittest
from unittest.mock import patch, MagicMock
from utils import download_multiple_scenarios_daily

class TestDownloadMultipleScenariosDaily(unittest.TestCase):
    
    @patch('utils.download_cmip6_netcdf_daily')
    def test_download_multiple_scenarios_daily(self, mock_download_cmip6):
        start_year = 2020
        end_year = 2021
        model = 'ACCESS-ESM1-5' ## not designed to handle multiple models unless iterated through a list via something like a loop
        variables = ['tasmax']
        scenarios = ['ssp126','ssp245'] ## can only test for either one historical or multiple future scenrios # as this function is designed to handle dates in an "either-or" manner ## which means it can only call historical or future at a time but never both # the function download_multiple_models_daily actually uses this function (download_multiple_scenarios_daily) twice separately within its framework and that is how it can download both historical and future at one go ###
        output_folder = '/tmp/output'

        mock_download_cmip6.return_value = None
        
        download_multiple_scenarios_daily(start_year, end_year, model, variables, scenarios, output_folder)
        
        self.assertEqual(mock_download_cmip6.call_count, 2) ## this is the expected number of calls it will make, once for historical and once for future

if __name__ == '__main__':
    unittest.main()
