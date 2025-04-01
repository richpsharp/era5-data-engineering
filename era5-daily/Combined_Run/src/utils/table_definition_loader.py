"""Load and manage Spark table definitions from YAML-formatted files."""

import os

from databricks.sdk.runtime import spark
import yaml

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
)

_type_mapping = {
    "string": StringType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def load_table_struct(yaml_path, table_name):
    """Loads a Spark StructType schema definition for a table from a YAML file.

    Args:
        yaml_path (str): Absolute or relative path to the YAML file containing
            table definitions. Example: '/path/to/table_definitions.yml'
        table_name (str): Name of the table to load the schema definition for.

    Returns:
        tuple:
            StructType: Spark StructType object defining the table's columns
                and data types.
            dict: Table metadata including database and schema names.

    Raises:
        FileNotFoundError: If the YAML file does not exist at the specified
            path.
        KeyError: If the YAML file is missing required keys
            ('tables', table_name, column details).
    """
    definition_path = os.path.abspath(yaml_path)
    if not os.path.exists(definition_path):
        raise FileNotFoundError(
            f"Definition file not found at: {definition_path}"
        )

    with open(definition_path, "r") as file:
        schema_def = yaml.safe_load(file)

    # we expect a "tables" section with "columns"
    if "tables" not in schema_def:
        raise KeyError(
            f'No "tables" key found in definition file: {definition_path}'
        )
    if table_name not in schema_def["tables"]:
        raise KeyError(
            f'Table "{table_name}" not found in definition file: {definition_path}'
        )
    table_info = schema_def["tables"][table_name]
    columns = table_info.get("columns", [])
    if not columns:
        raise KeyError(
            f'No "columns" list found for table "{table_name}" in {definition_path}'
        )

    # read all the columns, check that they are named
    struct_fields = []
    for col in columns:
        col_name = col.get("name")
        col_type = col.get("type")
        col_nullable = col.get("nullable", True)

        if not col_name or not col_type:
            raise KeyError(
                f'Column definition missing "name" or "type" in table '
                f'"{table_name}"'
            )
        if col_type not in _type_mapping:
            raise KeyError(
                f'Unknown column type "{col_type}" for "{col_name}" in table '
                f'"{table_name}"'
            )

        struct_fields.append(
            StructField(col_name, _type_mapping[col_type], col_nullable)
        )

    return StructType(struct_fields)


def create_table(full_table_path, table_definition):
    """Creates a Delta table in Databricks if it does not already exist.

    Args:
        full_table_path (str): Fully qualified table name in the format
            '<catalog>.<schema>.<table>'.
            Example: 'analytics_catalog.bronze_schema.era5_inventory'
        table_definition (StructType): Spark StructType object defining the
            table's columns and data types.

    Returns:
        None
    """
    column_definitions = ", ".join(
        f"{field.name} {field.dataType.simpleString()}"
        for field in table_definition.fields
    )
    print(f"Creating table {full_table_path} with columns: {column_definitions}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_path} ({column_definitions}) "
        f"USING DELTA"
    )
