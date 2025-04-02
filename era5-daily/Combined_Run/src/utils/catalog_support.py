"""Utilities to normalize catalog/schema access across jobs."""

from databricks.sdk.runtime import spark

_DEFAULT_LOCAL_CATALOG = "sandbox"
_DEFAULT_LOCAL_SCHEMA = "sandbox_schema"

# These are the catalog ids in the default databricks workbooks
_WORKBOOK_CATALOGS = ("spark_catalog", "hive_metastore")


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
