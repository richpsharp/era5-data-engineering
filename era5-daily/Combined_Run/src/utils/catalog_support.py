"""Utilities to normalize catalog/schema access across jobs."""

import logging

from databricks.sdk.runtime import spark
from pyspark.sql import SparkSession

# Get the active catlog, but make sure that the spark session is active
# we use this when doing sql calls and we need the catalog the current
# workbook/dab/python file is operating in
_DEFAULT_CATALOG = None
spark = SparkSession.getActiveSession()  # noqa: F811, keep it lc on purpose
if spark is not None:
    _DEFAULT_CATALOG = spark.catalog.currentCatalog()


_DEFAULT_LOCAL_CATALOG_SCHEMA = "sandbox.sandbox"

# These are the catalog ids in the default databricks workbooks
_WORKBOOK_CATALOGS = ("spark_catalog", "hive_metastore")


LOGGER = logging.getLogger(__name__)


def get_catalog_schema_fqdn(catalog_schema=None):
    """Wrapper to handle running in a job or workbook that has no args."""
    if catalog_schema:
        return catalog_schema
    return _DEFAULT_LOCAL_CATALOG_SCHEMA


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

    # we're in a module and might not have dbutils defined in context,
    # this does it manually
    try:
        dbutils  # Check if dbutils is defined
    except NameError:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)

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
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_fqdn} TO `Admin`")
    except Exception as e:
        LOGGER.warning(f"Not all PRIVILEGES were able to be set: {e}")
