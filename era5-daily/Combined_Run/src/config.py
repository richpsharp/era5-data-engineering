"""Global configurations for the era5 DAB."""
import os

ERA5_INVENTORY_TABLE_DEFINITION_PATH = "./era5_table_definitions.yml"
ERA5_INVENTORY_TABLE_NAME = "era5_gwsc_staging_inventory_table_v2"
AER_VOLUME_ROOT_PATH = "/Volumes/aer-processed"
ERA5_SOURCE_VOLUME_PATH = os.path.join(AER_VOLUME_ROOT_PATH, "era5")
ERA5_STAGING_VOLUME_ID = "era5_gwsc_staging_folder_v2"
ERA5_SOURCE_VOLUME_FQDN = "aer-processed.era5.daily_summary"

# These are set with the databricks command line:
# databricks secrets put-secret gwsc-secrets <aws_access_key|aws_secret_key> --string-value <KEYSTRING> --profile era5-service-principal
AWS_SECRET_SCOPE = "gwsc-secrets"
AWS_SECRET_KEY_ID = "aws_secret_key"
AWS_ACCESS_KEY_ID = "aws_access_key"
