import os
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


def load_schema(schema_yaml_path, table_name):
    """
    Loads a Spark StructType schema for the given table_name from a YAML file
    specified by the SCHEMA_YAML_PATH environment variable.

    :param table_name: (str) Name of the table whose schema to load
    :return: (StructType) Spark DataFrame schema
    :raises:
        ValueError: If SCHEMA_YAML_PATH is not set
        FileNotFoundError: If the YAML file doesn't exist
        KeyError: If the 'tables' key, the table_name, or a valid 'type' are missing in the YAML
    """
    schema_path = os.path.abspath(schema_yaml_path)
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")

    with open(schema_path, "r") as file:
        schema_def = yaml.safe_load(file)

    # check the layout of the schema.yml, expect a "tables" section with "columns"
    if "tables" not in schema_def:
        raise KeyError(f'No "tables" key found in schema file: {schema_path}')
    if table_name not in schema_def["tables"]:
        raise KeyError(
            f'Table "{table_name}" not found in schema file: {schema_path}'
        )
    table_info = schema_def["tables"][table_name]
    columns = table_info.get("columns", [])
    if not columns:
        raise KeyError(
            f'No "columns" list found for table "{table_name}" in {schema_path}'
        )

    # read all the columns, check that they are named
    struct_fields = []
    for col in columns:
        col_name = col.get("name")
        col_type = col.get("type")
        col_nullable = col.get("nullable", True)

        if not col_name or not col_type:
            raise KeyError(
                f'Column definition missing "name" or "type" in table "{table_name}"'
            )
        if col_type not in _type_mapping:
            raise KeyError(
                f'Unknown column type "{col_type}" for "{col_name}" in table "{table_name}"'
            )

        struct_fields.append(
            StructField(col_name, _type_mapping[col_type], col_nullable)
        )

    return StructType(struct_fields)


def create_table(table_info, table_schema):

    ddl = ", ".join(
        f"{field.name} {field.dataType.simpleString()}"
        for field in table_schema.fields
    )
    full_table_name = f"{table_info['database']}.{table_info['schema']}.{ERA5_INVENTORY_TABLE_NAME}"

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} ({ddl}) USING DELTA"
    )
