def test_download_multiple_models_daily(self, mock_download_multiple_scenarios):
    start_year = 1950
    end_year = 2100
    models = ['ACCESS-ESM1-5', 'BCC-CSM2-MR', 'ACCESS-CM2', 'CanESM5']
    variables = ['tas', 'pr', 'tasmin', 'tasmax']
    scenarios = ['historical', 'ssp126', 'ssp370', 'ssp585']
    output_folder = '/Volumes/cmip6-daily-data/gwsc-cmip6-daily/nex-gddp-cmip6'
    num_workers = 30
    
    mock_download_multiple_scenarios.return_value = None
    
    download_multiple_models_daily(start_year, end_year, models, variables, scenarios, output_folder, num_workers)
    
    # Adjust the expected_calls based on the exact behavior of the function
    expected_calls = 8  ## 8 calls (4 models * 2 calls) (One call for historical period and other call for future scenarios, this is how the download_multiple_scenarios_daily function is written to perform which is what we are mocking here)
    actual_calls = mock_download_multiple_scenarios.call_count
    self.assertEqual(actual_calls, expected_calls)




