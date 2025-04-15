"""Utilities to normalize catalog/schema access across jobs."""

import logging

_DEFAULT_LOCAL_CATALOG_SCHEMA = "sandbox.sandbox"

LOGGER = logging.getLogger(__name__)


def get_catalog_schema_fqdn(catalog_schema=None):
    """Wrapper to handle running in a job or workbook that has no args."""
    if catalog_schema:
        return catalog_schema
    return _DEFAULT_LOCAL_CATALOG_SCHEMA


def create_schema_if_not_exists(spark, schema_fqdn):
    """Create a Databricks schema if it doesn't exist and grant full privileges.

    This function creates a schema identified by its fully qualified name
    (e.g., "catalog.schema") if it does not already exist. It then grants full
    control on the schema to the current principal executing this code
    (via SQL's current_user() function) and to the Admin.

    Args:
        spark (SparkSession): Active SparkSession.
        schema_fqdn (str): Fully qualified schema name "catalog.schema".

    Returns:
        None
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqdn}")

    current_principal = spark.sql("SELECT current_user() AS user").collect()[0][
        "user"
    ]

    # Grant full privileges on the schema to the current principal and to Admin.
    try:
        spark.sql(
            f"GRANT ALL PRIVILEGES ON SCHEMA {schema_fqdn} TO "
            f"{current_principal}"
        )
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_fqdn} TO `Admin`")
    except Exception as e:
        LOGGER.warning(f"Not all privileges were able to be set: {e}")
