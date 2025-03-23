import xarray as xr
import cdsapi



c = cdsapi.Client()

import cdsapi

dataset = "derived-era5-land-daily-statistics"
request = {
    "variable": ["2m_temperature"],
    "year": "1950",
    "month": "01",
    "day": ["01"],
    "daily_statistic": "daily_mean",
    "time_zone": "utc+00:00",
    "frequency": "6_hourly",
    'format': 'grib'
}

client = cdsapi.Client()
r = client.retrieve(dataset, request)
help(r)

ds = xr.open_dataset('3f411f670d1a2849e648a6d866f130ed.nc')
print(ds)

# # Example for ERA5 single-level data:
# dataset = "derived-era5-land-daily-statistics"
# request = {
#     "variable": ["2m_temperature"],
#     "year": "1950",
#     "month": "01",
#     "day": ["01"],
#     "daily_statistic": "daily_mean",
#     "time_zone": "utc+00:00"
# }
# c.retrieve(dataset, request, 'era5_single_level.grib').download()

# # To get NetCDF instead, simply set 'format': 'netcdf' and adjust the output filename:
# c.retrieve(
#     'derived-era5-land-daily-statistics',
#     {
#         'product_type': 'reanalysis',
#         'format': 'netcdf',  # note: request NetCDF format
#         'variable': [
#             '2m_temperature',
#         ],
#         'year': '2020',
#         'month': '01',
#         'day': '01',
#         'time': '12:00',
#     },
#     'era5_single_level.nc'
# )
