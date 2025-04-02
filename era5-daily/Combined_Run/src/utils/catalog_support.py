"""Utilities to normalize catalog/schema access across jobs."""

import logging

from databricks.sdk.runtime import spark

_DEFAULT_LOCAL_CATALOG = "sandbox"
_DEFAULT_LOCAL_SCHEMA = "sandbox_schema"

# These are the catalog ids in the default databricks workbooks
_WORKBOOK_CATALOGS = ("spark_catalog", "hive_metastore")


LOGGER = logging.getLogger(__name__)


def get_catalog_schema_fqdn():
    """Returns an FQDN table path for Databricks.

    If this is called in a "workbook" environment the catalog will default to
    `DEFAULT_LOCAL_CATALOG` and the schema to `DEFAULT_LOCAL_SCHEMA`, otherwise
    will use the active catalog and schema the job is running in.

    Args:
        spark (SparkSession): Active SparkSession.

    Returns:
        str: Fully qualified table path in '<catalog>.<schema>.<table>' format.
    """
    catalog = spark.catalog.currentCatalog()
    if catalog in _WORKBOOK_CATALOGS:
        catalog = _DEFAULT_LOCAL_CATALOG
        schema = _DEFAULT_LOCAL_SCHEMA
    else:
        schema = spark.catalog.currentDatabase()

    return f"{catalog}.{schema}"


def create_schema_if_not_exists(schema_fqdn):
    """Create a Databricks schema if it doesn't exist and grant full privileges.

    This function creates a schema identified by its fully qualified name (e.g.,
    "catalog.schema") if it does not already exist. It then grants full control
    on the schema to both the current principal executing this code and to the
    admin.

    Args:
        schema_fqdn (str): Fully qualified schema name (e.g., "catalog.schema").

    Raises:
        Exception: Propagates any exceptions raised by the SQL commands.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqdn}")

    # Get the current principal (user) from the notebook context.
    current_principal = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )

    # Grant full privileges on the schema to the current principal and admin.
    try:
        spark.sql(
            f"GRANT ALL PRIVILEGES ON SCHEMA {schema_fqdn} TO "
            f"`{current_principal}`"
        )
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_fqdn} TO `admin`")
    except Exception as e:
        LOGGER.warning(f"Not all PRIVILEGES were able to be set: {e}")
