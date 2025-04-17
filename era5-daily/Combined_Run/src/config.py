"""Global configurations for the era5 DAB."""

import os

BASE_DIR = os.path.dirname(__file__)

ERA5_INVENTORY_TABLE_DEFINITION_PATH = os.path.join(
    BASE_DIR, "./era5_table_definitions.yml"
)
ERA5_INVENTORY_TABLE_NAME = "era5_staging_inventory_bronze"
ERA5_SOURCE_VOLUME_PATH = "/Volumes/aer-processed/era5"
ERA5_STAGING_VOLUME_ID = "era5_staging_folder_bronze"
ERA5_SOURCE_VOLUME_FQDN = "aer-processed.era5.daily_summary"
# This is the nvme location if you use an i4.* node
LOCAL_EPHEMERAL_PATH = "/local_disk0/workspace"
DEFAULT_LOCAL_CATALOG_FQDN = "sandbox.sandbox"

# This is a special catalog with a few files in it for the test suite
ERA5_TEST_DATA_PATH = "/Volumes/test-data/era5-test"
