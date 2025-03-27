import yaml
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

_type_mapping = {
    "string": StringType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def load_schema(table_name, yaml_path="../schemas/schemas.yaml"):
    with open(yaml_path, 'r') as file:
        schema_def = yaml.safe_load(file)

    table_info = schema_def['tables'][table_name]

    struct_fields = [
        StructField(
            col["name"],
            _type_mapping[col["type"]],
            col["nullable"]
        ) for col in table_info["columns"]
    ]

    return StructType(struct_fields)
