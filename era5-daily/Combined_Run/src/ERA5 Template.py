# Databricks notebook source
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

result = dbutils.notebook.run(
    workbook_path,
    0,
    arguments=arguments)

result.load()
print('Mean 2D numpy array:')
result.plot(x='longitude', y='latitude')  # or whatever your coordinate names are

title = f'{arguments["variable"]} {arguments["agg_fn"]} {start_date} to {end_date}'
plt.title(title)
plt.show()

