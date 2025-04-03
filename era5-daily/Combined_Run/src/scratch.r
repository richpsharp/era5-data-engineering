# Databricks notebook source
# MAGIC %python
# MAGIC import time
# MAGIC
# MAGIC from utils.file_utils import copy_file_to_mem
# MAGIC from utils.file_utils import copy_mem_file_to_path
# MAGIC from utils.file_utils import hash_bytes
# MAGIC from utils.file_utils import is_netcdf_file_valid
# MAGIC
# MAGIC path = '/Volumes/aer-processed/era5/daily_summary/reanalysis-era5-sfc-daily-1950-12-12.nc'
# MAGIC file_in_mem = copy_file_to_mem(path)
# MAGIC start = time.time()
# MAGIC result = is_netcdf_file_valid(file_in_mem, path)
# MAGIC print(f'{result} in {time.time()-start:.2f}s')
# MAGIC
