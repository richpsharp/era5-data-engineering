"""Utilities to normalize catalog/schema access across jobs."""

from databricks.sdk.runtime import spark

_DEFAULT_LOCAL_CATALOG = "sandbox"
_DEFAULT_LOCAL_SCHEMA = "sandbox_schema"
# by experiment and some documentaiton these appear to be the catalogs that show up
# if code is run direclty in a workbook. this is used to detect the default.
_WORKBOOK_CATALOGS = ("spark_catalog", "hive_metastore")


def resolve_table_path(table_name):
    """Returns an FQDN table path for Databricks.

    If this is called in a "workbook" environment the catalog will default to
    `DEFAULT_LOCAL_CATALOG` and the schema to `DEFAULT_LOCAL_SCHEMA`, otherwise
    will use the active catalog and schema the job is running in.

    Args:
        spark (SparkSession): Active SparkSession.
        table_name (str): Name of the table.

    Returns:
        str: Fully qualified table path in '<catalog>.<schema>.<table>' format.
    """
    catalog = spark.catalog.currentCatalog()
    if catalog in _WORKBOOK_CATALOGS:
        catalog = _DEFAULT_LOCAL_CATALOG
        schema = _DEFAULT_LOCAL_SCHEMA
    else:
        schema = spark.catalog.currentDatabase()

    return f"{catalog}.{schema}.{table_name}"
