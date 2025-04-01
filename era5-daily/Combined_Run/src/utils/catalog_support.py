# catalog_support.py

_DEFAULT_LOCAL_CATALOG = 'sandbox'
_DEFAULT_LOCAL_SCHEMA = 'sandbox_schema'
# by experiment and some documentaiton these appear to be the catalogs that show up 
# if code is run direclty in a workbook. this is used to detect the default.
_WORKBOOK_CATALOGS = ('spark_catalog', 'hive_metastore')

def resolve_table_path(
    spark,
    table_name
):
    """
    Returns a fully qualified table path including catalog, schema, and table name.

    If this is called in a "workbook" environment the catalog will default to DEFAULT_LOCAL_CATALOG
    and the schema to DEFAULT_LOCAL_SCHEMA. 

    Falls back to provided defaults if running locally (spark_catalog).

    Parameters:
        spark (SparkSession): Active SparkSession.
        table_name (str): Name of the table.
        fallback_catalog (str): Catalog used if the current catalog is 'spark_catalog'.
        fallback_schema (str): Schema used if the current catalog is 'spark_catalog'.

    Returns:
        str: Fully qualified table path in '<catalog>.<schema>.<table>' format.
    """
    catalog = spark.catalog.currentCatalog()
    if catalog in _WORKBOOK_CATALOGS:
        catalog = _DEFAULT_LCOAL_CATALOG
        schema = _DEFAULT_LOCAL_SCHEMA
    else:
        schema = spark.catalog.currentDatabase()

    return f"{catalog}.{schema}.{table_name}"
