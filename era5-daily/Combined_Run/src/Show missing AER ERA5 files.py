# Databricks notebook source
import os
import glob
import datetime

def find_missing_date_ranges(directory, start_date_str, end_date_str,
                             prefix='reanalysis-era5-sfc-daily-',
                             suffix='.nc'):
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

    all_dates = set()
    day = start_date
    while day <= end_date:
        all_dates.add(day)
        day += datetime.timedelta(days=1)

    # Glob all NetCDF files
    pattern = os.path.join(directory, f'{prefix}*{suffix}')
    files = glob.glob(pattern)
    for f in files:
        base = os.path.basename(f)
        date_part = base.replace(prefix, '').replace(suffix, '')
        try:
            file_date = datetime.datetime.strptime(date_part, '%Y-%m-%d').date()
            if file_date in all_dates:
                all_dates.remove(file_date)
        except ValueError:
            pass  
    missing_dates = sorted(all_dates)

    if not missing_dates:
        return []

    ranges = []
    range_start = missing_dates[0]
    range_end = missing_dates[0]

    for d in missing_dates[1:]:
        if d == range_end + datetime.timedelta(days=1):
            range_end = d
        else:
            ranges.append((range_start, range_end))
            range_start = d
            range_end = d
    ranges.append((range_start, range_end))
    return ranges

directory = '/Volumes/aer-processed/era5/daily_summary'
start_str = '1950-01-01'
end_str = '2025-03-23'

missing_ranges = find_missing_date_ranges(directory, start_str, end_str)
print(missing_ranges)

