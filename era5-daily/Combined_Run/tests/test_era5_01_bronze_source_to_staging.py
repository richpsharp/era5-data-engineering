"""Test suite for era5_01_bronze_source_to_staging.py"""

import sys
from src import era5_01_bronze_source_to_staging
from src.config import ERA5_TEST_DATA_PATH


def test_main(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["prog", "--catalog_schema", "test.test_schema"]
    )
    # change config.ERA5_SOURCE_VOLUME_PATH to point at /Volumes/test_suite/sample_era5_data

    monkeypatch.setattr(
        era5_01_bronze_source_to_staging,
        "ERA5_SOURCE_VOLUME_PATH",
        ERA5_TEST_DATA_PATH,  # now pipeline.source_directory = "<sample>/daily_summary"
    )
    era5_01_bronze_source_to_staging.main()
    # verify that {args.catalog_schema}.(ERA5_INVENTORY_TABLE_NAME) has the entries from the volume
    # verify that (f"{args.catalog_schema}.{ERA5_STAGING_VOLUME_ID}).replace('.', '/') has those files copied to them
