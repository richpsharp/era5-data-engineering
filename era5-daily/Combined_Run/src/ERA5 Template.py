# Databricks notebook source
# MAGIC %pip install rioxarray
# MAGIC %restart_python

# COMMAND ----------

import rioxarray
import matplotlib.pyplot as plt

workbook_path = '/Workspace/Users/rpsharp@ua.edu/richpsharp fork -- era5-data-engineering/era5-daily/Combined_Run/src/ERA5 Monthly Normals REST'

start_date = '2023-01-01'
end_date = '2023-02-01'
dataset = 'era5'
agg_fn = 'mean'

arguments = {
    'start_date': str(start_date),
    'end_date': str(end_date),
    'dataset': 'era5',
    'agg_fn': 'mean',
    'variable': 'era5.mean_t2m_c'
}

dbfs_tif_path = dbutils.notebook.run(
    workbook_path,
    0,
    arguments=arguments)

local_tif_path = dbfs_tif_path.replace('dbfs:/', '/dbfs/')

# 3) Open it as a rioxarray DataArray
da = rioxarray.open_rasterio(local_tif_path)

# 4) If it's a single-band GeoTIFF, da will have shape (band, y, x).
#    We can just select band=0 to get a 2D array:
da_2d = da.isel(band=0)

# 5) Plot
title = f'{arguments["variable"]} {arguments["agg_fn"]} {start_date} to {end_date}'
da_2d.plot()
plt.title(title)
plt.show()

