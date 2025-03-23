# Databricks notebook source
dbutils.widgets.dropdown('agg_fn', 'mean', ['mean', 'sum', 'count'], 'Aggregation')
dbutils.widgets.text('end_date', '2020-12-31', 'End Date')
dbutils.widgets.text('start_date', '2020-01-01', 'Start Date')
dbutils.widgets.dropdown('dataset', 'era5', ['era5', 'cmip6'], 'Dataset')


# COMMAND ----------

import os
import re
from datetime import datetime
import glob

start_date = datetime.strptime(dbutils.widgets.get('start_date'), '%Y-%m-%d').date()
end_date = datetime.strptime(dbutils.widgets.get('end_date'), '%Y-%m-%d').date()

pattern = re.compile(r'.*(\d{4}-\d{2}-\d{2})\.nc$')

def valid_date(filepath):
    basename = os.path.basename(filepath)
    match = pattern.match(basename)
    if match:
        file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
        if start_date <= file_date <= end_date:
            return filepath

era5_path_list = [
    p for p in 
    glob.glob('/Volumes/aer-processed/era5/daily_summary/*.nc')
    if valid_date(p)]
print(era5_path_list[:10])
print(len(era5_path_list))


# COMMAND ----------

from datetime import datetime
import xarray as xr
import psutil
import os
import shutil
import time
import glob
import threading
import concurrent.futures

start_date = datetime.strptime(dbutils.widgets.get('start_date'), '%Y-%m-%d').date()
end_date = datetime.strptime(dbutils.widgets.get('end_date'), '%Y-%m-%d').date()
agg_fn = dbutils.widgets.get('agg_fn')


CACHE_DIR = '/local_disk0/era5'

def copy_file(file_path):
    shutil.copy(file_path, CACHE_DIR)
    target_path = os.path.join(CACHE_DIR, file_path)
    size = os.path.getsize(target_path)
    try:
        ds = xr.open_dataset(target_path)
        ds.load()
        ds.close()
        return file_path, size, None
    except Exception as exception:
        try:
            os.remove(target_path)
        except:
            pass
        return file_path, size, exception


def main():
    max_workers = 1
    offset = 365
    index = 0
    os.makedirs(CACHE_DIR, exist_ok=True)

    start = time.time()
    era5_path_list = [
        p for p in 
        glob.glob('/Volumes/aer-processed/era5/daily_summary/*.nc')
        if not os.path.exists(p)]
    print(f'took {time.time()-start:.2f}s to read {len(era5_path_list)} files', flush=True)

    batch_start = time.time()
    total_bytes_copied = 0
    total_completed = 0
    bandwidth = 0
    gbps = 0
    approx_time_left = 0

    print(f'max workers: {max_workers} -- ', end='', flush=True)

    #monitor_thread = threading.Thread(target=monitor_cpu, daemon=True)
    #monitor_thread.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        print('submitting copy commands', flush=True)
        futures = [executor.submit(copy_file, fp) for fp in era5_path_list]

        for f in concurrent.futures.as_completed(futures):
            file_path, file_size, exception = f.result()
            if exception:
                print(f'error when copying {file_path}: {exception}')
            total_completed += 1
            total_bytes_copied += file_size
            elapsed = time.time() - batch_start
            files_left = len(era5_path_list) - total_completed
            if elapsed > 0 and total_completed > 0:
                bandwidth = (total_bytes_copied / (1024 * 1024)) / elapsed
                gbps = (total_bytes_copied * 8 / (1024 * 1024 * 1024)) / elapsed
                avg_time_per_file = elapsed / total_completed
                approx_time_left = avg_time_per_file * files_left

            print(
                f'Files left: {files_left}, approx time left: {approx_time_left:.2f}s, bandwidth: {bandwidth:.2f} MB/s ({gbps:.2f} Gbps)', flush=True)

    SHOULD_STOP = True

if __name__ == '__main__':
    main()



# COMMAND ----------

!ls /local_disk0/era5
