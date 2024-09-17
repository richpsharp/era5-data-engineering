import os
import s3fs
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm




def download_file(file_path, fs, output_path):
    """
    Downloads a file from S3 and saves it locally.

    Parameters
    ----------
    file_path : str
        The S3 path to the file.
    fs : s3fs.S3FileSystem
        The S3 filesystem object.
    output_path : str
        The local path to save the file to.

    Raises
    ------
    Exception
        If there is an error during the file download.
    """
    try:
        with fs.open(file_path, mode='rb') as f:
            with open(output_path, 'wb') as out_file:
                out_file.write(f.read())
        print(f"Saved file to {output_path}")
    except Exception as e:
        print(f"Failed to download file {file_path}: {e}")


def download_cmip6_netcdf_daily(start_year, end_year, model, scenario, variables, output_folder, num_workers=None):
    """
    Downloads CMIP6 NetCDF daily files from an S3 bucket and saves them locally,
    including files with additional version strings '_v1.1' and '_v1.2' if available.

    Parameters
    ----------
    start_year : int
        The start year for the data download.
    end_year : int
        The end year for the data download.
    model : str
        The climate model to download data for. Must be one of the valid models.
    scenario : str
        The scenario to download data for. Must be one of 'historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585'.
    variables : list of str
        The list of variables to download data for.
    output_folder : str
        The local directory where the downloaded files will be saved.
    num_workers : int, optional
        The number of parallel workers to use for downloading files. Default is 1.

    Raises
    ------
    ValueError
        If the model or scenario is not valid.
    """
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError as e:
        raise ValueError(f"Invalid year provided. Start year and end year must be integers. {e}")

    num_workers = int(num_workers) if num_workers is not None else 1

    valid_models = ['ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CanESM5', 'CMCC-ESM2', 'FGOALS-g3', 
                    'GISS-E2-1-G', 'MIROC-ES2L', 'MPI-ESM1-2-HR', 'MRI-ESM2-0', 'NESM3',
                    'NorESM2-MM', 'CNRM-ESM2-1', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 
                    'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM']

    if model not in valid_models:
        raise ValueError(f"Invalid model '{model}'. Choose from {valid_models}.")

    valid_scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    if scenario not in valid_scenarios:
        raise ValueError(f"Invalid scenario '{scenario}'. Choose from {valid_scenarios}.")

    fs = s3fs.S3FileSystem(anon=True)

    ensemble_mapping = {
        "FGOALS-g3": "r3i1p1f1",
        "GISS-E2-1-G": "r1i1p1f2",
        "MIROC-ES2L": "r1i1p1f2",
        "CNRM-ESM2-1": "r1i1p1f2"
    }
    ensemble = ensemble_mapping.get(model, "r1i1p1f1")

    def get_base_path(variable):
        """
        Constructs the S3 base path for a given variable.

        Parameters
        ----------
        variable : str
            The variable to construct the path for.

        Returns
        -------
        str
            The S3 base path.
        """
        return f's3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/{scenario}/{ensemble}/{variable}/'

    version_suffixes = ['', '_v1.1', '_v1.2']  # Version suffixes to search for

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for variable in variables:
            base_path = get_base_path(variable)
            futures = []
            progress_bar = tqdm(total=end_year - start_year + 1, desc=f"Downloading {variable} for {model} {scenario}")
            for year in range(start_year, end_year + 1):
                for suffix in version_suffixes:
                    file_path = f"{base_path}{variable}_day_{model}_{scenario}_{ensemble}_gn_{year}{suffix}.nc"
                    output_path = os.path.join(output_folder, model, scenario, variable, f"{variable}_day_{model}_{scenario}_{ensemble}_gn_{year}{suffix}.nc")
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)

                    # Submit each file download task to the thread pool
                    future = executor.submit(download_file, file_path, fs, output_path)
                    futures.append(future)

            # Wait for all download tasks to complete
            for future in as_completed(futures):
                future.result()  # Propagate any exceptions raised during the download
                progress_bar.update(1)
            progress_bar.close()

    print(f"All datasets for model {model} and scenario {scenario} downloaded successfully.")


def download_multiple_scenarios_daily(start_year, end_year, model, variables, scenarios, output_folder, num_workers=None, log_func=None):
    """
    Downloads CMIP6 NetCDF daily files for multiple scenarios and saves them locally.

    Parameters
    ----------
    start_year : int
        The start year for the data download.
    end_year : int
        The end year for the data download.
    model : str
        The climate model to download data for. Must be one of the valid models.
    variables : list of str
        The list of variables to download data for.
    scenarios : list of str
        The list of scenarios to download data for. Must be one of 'historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585'.
    output_folder : str
        The local directory where the downloaded files will be saved.
    num_workers : int, optional
        The number of parallel workers to use for downloading files. Default is 4.
    log_func : callable, optional
        A function to handle logging messages. If not provided, messages will be printed to the console.

    Raises
    ------
    Exception
        If there is an error during the data download for a scenario.

    Examples
    --------
    >>> download_multiple_scenarios_daily(2020, 2025, 'GISS-E2-1-G', ['tasmax', 'tasmin'], ['ssp126', 'ssp245'], '/path/to/output')
    """
    # Ensure start_year and end_year are integers
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError as e:
        raise ValueError(f"Invalid year provided. Start year and end year must be integers. {e}")

    for scenario in scenarios:
        if (scenario == 'historical' and end_year <= 2014) or (scenario != 'historical' and start_year >= 2015):
            key = f"{model}_{scenario}"
            message = f"Starting processing for: {key}"
            if log_func:
                log_func(message)
            else:
                print(message)
            try:
                download_cmip6_netcdf_daily(start_year, end_year, model, scenario, variables, output_folder, num_workers=num_workers)
                message = f"Data processed for: {key}"
                if log_func:
                    log_func(message)
                else:
                    print(message)
            except Exception as e:
                message = f"Exception while processing {key}: {str(e)}"
                if log_func:
                    log_func(message)
                else:
                    print(message)

def download_multiple_models_daily(start_year, end_year, models, variables, scenarios, output_folder, num_workers=None, log_func=None):
    """
    Downloads CMIP6 NetCDF daily files for multiple models and scenarios, saving them locally.

    Parameters
    ----------
    start_year : int
        The start year for the data download.
    end_year : int
        The end year for the data download.
    models : list of str
        The list of climate models to download data for. Each must be one of the valid models.
    variables : list of str
        The list of variables to download data for.
    scenarios : list of str
        The list of scenarios to download data for. Must be one of 'historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585'.
    output_folder : str
        The local directory where the downloaded files will be saved.
    num_workers : int, optional
        The number of parallel workers to use for downloading files. Default is 4.
    log_func : callable, optional
        A function to handle logging messages. If not provided, messages will be printed to the console.

    Raises
    ------
    Exception
        If there is an error during the data download for a model.

    Examples
    --------
    >>> download_multiple_models_daily(2020, 2025, ['GISS-E2-1-G', 'MIROC-ES2L'], ['tasmax', 'tasmin'], ['ssp126', 'ssp245'], '/path/to/output')
    """
    # Ensure start_year and end_year are integers
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError as e:
        raise ValueError(f"Invalid year provided. Start year and end year must be integers. {e}")

    total_tasks = len(models) * (len(scenarios) if end_year >= 2015 else 1)
    with tqdm(total=total_tasks, desc="Overall Progress") as overall_progress_bar:
        for model in models:
            message = f"Starting processing for model: {model}"
            if log_func:
                log_func(message)
            else:
                print(message)
            
            try:
                if start_year <= 2014:
                    download_multiple_scenarios_daily(start_year, min(end_year, 2014), model, variables, ['historical'], output_folder, num_workers=num_workers, log_func=log_func)
                if end_year >= 2015:
                    download_multiple_scenarios_daily(max(start_year, 2015), end_year, model, variables, ['ssp126', 'ssp245', 'ssp370', 'ssp585'], output_folder, num_workers=num_workers, log_func=log_func)
                message = f"Finished processing for model: {model}"
                if log_func:
                    log_func(message)
                else:
                    print(message)
            except Exception as e:
                message = f"Exception while processing model {model}: {str(e)}"
                if log_func:
                    log_func(message)
                else:
                    print(message)
            overall_progress_bar.update(1)
