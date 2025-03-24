# Databricks notebook source
# MAGIC %pip install rioxarray rasterio dask
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

print('starting')
# Define all the parameterized inputs to this workbook
from enum import Enum

# A "None" default string to use for dropdowns/text that must be explicitly set
DEFAULT = 'None'

# Widget keys
AGG_FN = 'agg_fn'
END_DATE = 'end_date'
START_DATE = 'start_date'
DATASET = 'dataset'
VARIABLE = 'variable'

# These are the possible aggregation functions
class AggregationFunction(Enum):
    MEAN = 'mean'
    SUM = 'sum'
    COUNT = 'count'

# These are the possible datasets
class DatasetType(Enum):
    ERA5 = 'era5'
    CMIP6 = 'cmip6'

# These are the possible ERA5 variables
class ERA5Variables(Enum):
    MEAN_T2M_C = 'era5.mean_t2m_c'
    MAX_T2M_C = 'era5.max_t2m_c'
    MIN_T2M_C = 'era5.min_t2m_c'
    SUM_TP_MM = 'era5.sum_tp_mm'

# Paths
ROOT_PATHS = {
    DatasetType.ERA5.value: '/Volumes/aer-processed/era5/daily_summary/',
    DatasetType.CMIP6.value: '',
}

# Prepare widget definitions
input_vars = [
    (AGG_FN, [DEFAULT] + [a.value for a in AggregationFunction], 'Aggregation'),
    (END_DATE, None, 'End Date'),
    (START_DATE, None, 'Start Date'),
    (DATASET, [DEFAULT] + [d.value for d in DatasetType], 'Dataset'),
    (VARIABLE, [DEFAULT] + [v.value for v in ERA5Variables], 'Dataset Variable')
]

# Build Databricks widgets
for key, selection_list, label in input_vars:
    if selection_list is not None:
        dbutils.widgets.dropdown(key, DEFAULT, selection_list, label)
    else:
        dbutils.widgets.text(key, DEFAULT, label)
        
def get_inputs():
    args = {}
    error_list = []
    for key, valid_list, _ in input_vars:
        args[key] = dbutils.widgets.get(key)
        if args[key] == DEFAULT or (valid_list is not None and args[key] not in valid_list):
            error_list.append(f'Please provide a value for {key}/{label}.')

    if error_list:
        raise ValueError('\n'.join(error_list))
    return args

# COMMAND ----------

import shutil
import json
import os
import re
from datetime import datetime
import glob
import time
import concurrent.futures
import psutil
import xarray as xr
import matplotlib.pyplot as plt

ARGS = get_inputs()
print(ARGS)

start_date = datetime.strptime(ARGS[START_DATE], '%Y-%m-%d').date()
end_date = datetime.strptime(ARGS[END_DATE], '%Y-%m-%d').date()

if start_date > end_date:
    raise ValueError(f'start date: {start_date} is later than end date: {end_date}')
pattern = re.compile(r'.*(\d{4}-\d{2}-\d{2})\.nc$')

def valid_date(filepath):
    basename = os.path.basename(filepath)
    match = pattern.match(basename)
    if match:
        file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
        if start_date <= file_date <= end_date:
            return filepath

dataset_root_dir = ROOT_PATHS[ARGS[DATASET]]
remote_raster_path_list = [
    p for p in 
    glob.glob(os.path.join(dataset_root_dir, '*.nc'))
    if valid_date(p)]
print(len(remote_raster_path_list), flush=True)
        
CACHE_DIR = f'/local_disk0/{ARGS[DATASET]}'

def copy_file(file_path):
    try:
        target_path = os.path.join(CACHE_DIR, file_path)
        preexists = True
        if not os.path.exists(target_path):
            shutil.copy(file_path, target_path)
            preexists = False
            
        size = os.path.getsize(target_path)
        return file_path, size, preexists, None
    except Exception as exception:
        print(f'error: {exception}')
        try:
            os.remove(target_path)
        except:
            pass
        return file_path, size, preexists, exception

max_workers = os.cpu_count()*4
os.makedirs(CACHE_DIR, exist_ok=True)

batch_start = time.time()
total_bytes_copied = 0
total_completed = 0
bandwidth = 0
gbps = 0
approx_time_left = 0

print(f'max workers: {max_workers} -- ', end='', flush=True)
file_path_list = []
with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    print('submitting copy commands', flush=True)
    futures = [executor.submit(copy_file, fp) for fp in remote_raster_path_list]
    #x = [copy_file(fp) for fp in remote_raster_path_list]

    for f in concurrent.futures.as_completed(futures):
        file_path, file_size, pre_exists, exception = f.result()
        if exception:
            print(f'error when copying {file_path}: {exception}')
        file_path_list.append(file_path)
        if pre_exists:
            # don't update like we copied it or anything
            continue
        total_completed += 1
        total_bytes_copied += file_size
        elapsed = time.time() - batch_start
        files_left = len(remote_raster_path_list) - total_completed
        if elapsed > 0 and total_completed > 0:
            bandwidth = (total_bytes_copied / (1024 * 1024)) / elapsed
            gbps = (total_bytes_copied * 8 / (1024 * 1024 * 1024)) / elapsed
            avg_time_per_file = elapsed / total_completed
            approx_time_left = avg_time_per_file * files_left

        mem = psutil.virtual_memory()
        print(
            f'Files left: {files_left}, approx time left: {approx_time_left:.2f}s, bandwidth: {bandwidth:.2f} MB/s ({gbps:.2f} Gbps) ', 
            f'Memory usage: {mem.used/(1024*1024):.2f} MB used, '
            f'{mem.available/(1024*1024):.2f} MB free '
            f'({mem.percent:.2f}% used)', flush=True)
print('all done loading, now xr opening')
ds = xr.open_mfdataset(file_path_list, combine='by_coords')
print('dataset opened, calculating aggregation')
mean_2d = ds['mean_t2m_c'].mean(dim='time', skipna=True)

mean_2d = mean_2d.rio.write_crs("EPSG:4326", inplace=True)
geotiff_path = f'/tmp/{ARGS[VARIABLE]}_{ARGS[AGG_FN]}_{start_date}_to_{end_date}.tif'
print(geotiff_path)
mean_2d.rio.to_raster(geotiff_path)
dbfs_path = f'dbfs:{geotiff_path}'
dbutils.fs.cp(f'file:{geotiff_path}', dbfs_path)
dbutils.notebook.exit(dbfs_path)


# COMMAND ----------

import rioxarray
da = rioxarray.open_rasterio(geotiff_path)
da_2d = da.isel(band=0)
da_2d.plot()
plt.show()
