# Databricks notebook source
workbook_path = '/Workspace/Users/rpsharp@ua.edu/richpsharp fork -- era5-data-engineering/era5-daily/Combined_Run/src/ERA5 Monthly Normals REST'

start_date = '2020-01-01'
end_date = '2022-12-31'
dataset = 'era5'
agg_fn = 'mean'

result = dbutils.notebook.run(
    workbook_path,
    0,
    arguments={
        'start_date': str(start_date),
        'end_date': str(end_date),
        'dataset': str(dataset),
        'agg_fn': str(agg_fn)
})

