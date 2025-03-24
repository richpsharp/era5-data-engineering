# Databricks notebook source
import json
import geopandas as gpd
import rioxarray
import matplotlib.pyplot as plt
from shapely.geometry import mapping

workbook_path = '/Workspace/Users/rpsharp@ua.edu/richpsharp fork -- era5-data-engineering/era5-daily/Combined_Run/src/ERA5 Monthly Normals REST'

start_date = '2000-01-01'
end_date = '2014-12-31'
dataset = 'era5'
agg_fn = 'mean'
variable = 'era5.mean_t2m_c'
country_list = ['CAN', 'MEX', 'FRA']
gpkg_path = '/Volumes/global/global_datasets/gdam_country_gpkg/countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg'

arguments = {
    'start_date': str(start_date),
    'end_date': str(end_date),
    'dataset': dataset,
    'agg_fn': agg_fn,
    'variable': variable,
    'aoi_path': gpkg_path,
    'aoi_filter': json.dumps({"iso3": country_list}),
}

dbfs_tif_path = dbutils.notebook.run(
    workbook_path,
    0,
    arguments=arguments)

local_tif_path = dbfs_tif_path.replace('dbfs:/', '/dbfs/')
da = rioxarray.open_rasterio(local_tif_path)

gdf = gpd.read_file(arguments['aoi_path'])
for column_name, allowed_values in json.loads(arguments['aoi_filter']).items():
    gdf = gdf[gdf[column_name].isin(allowed_values)]

title = f'{arguments["variable"]} {arguments["agg_fn"]} {start_date} to {end_date}'
fig, ax = plt.subplots()
da.plot(ax=ax)
gdf.boundary.plot(ax=ax, edgecolor='black')
plt.title(title)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
