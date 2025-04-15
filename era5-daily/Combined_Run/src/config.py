"""Global configurations for the era5 DAB."""

ERA5_INVENTORY_TABLE_DEFINITION_PATH = "./era5_table_definitions.yml"
ERA5_INVENTORY_TABLE_NAME = "era5_gwsc_staging_inventory_table_v2"
ERA5_SOURCE_VOLUME_PATH = "/Volumes/aer-processed/era5"
ERA5_STAGING_VOLUME_ID = "era5_gwsc_staging_folder_v2"
ERA5_SOURCE_VOLUME_FQDN = "aer-processed.era5.daily_summary"
# This is the nvme location if you use an i3.* node
LOCAL_EPHEMERAL_PATH = "/local_disk0/workspace"
DEFAULT_LOCAL_CATALOG_FQDN = "sandbox.sandbox"
