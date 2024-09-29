# Databricks notebook source
# MAGIC %md

# MAGIC ## Checking if the silver table exists
# MAGIC
# MAGIC **If it does then do not run the rest of the code**

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the path or table name for the Delta table
table_name = "pilot.test_silver.esri_worldcountryboundaries_global_silver2"

# Check if the Delta table exists
if spark._jsparkSession.catalog().tableExists(table_name):
    print(f"The Delta table '{table_name}' already exists. Skipping the rest of the notebook.")
    
    # Skip the rest of the notebook
    import sys
    sys.exit(0)
else:
    print(f"The Delta table '{table_name}' does not exist. Proceeding with the notebook execution.")

# COMMAND ----------

# MAGIC %md

# MAGIC For maximum parallelism, the Ingest notebook split all multipolygons into just polygons. Then the Tessellate notebooke tessellated each polygon separately. Therefore, two chips of a country might lie in the same cell: imagine two nearby islands, or an island just off shore of primary land.
# MAGIC
# MAGIC It's easier to work with a tessellation of a country if (a) each H3 cell in the tessellation occurs just once and (b) includes all the chips of that country within that cell's boundary. Below, we use `mos.st_union_agg()` to union all the chips within each country's cells.
# MAGIC
# MAGIC Be aware that the union operation can fail (e.g. with non-noded intersections) if the chips in a cell cannot be combined into a valid multipolygon.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION # LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

## - spark configs
spark.conf.set("spark.databricks.geo.st.enabled", True)                # <-- turn on spatial sql
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <-- tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# COMMAND ----------

# -- mosaic --
import mosaic as mos
mos.enable_mosaic(spark, dbutils)

from pyspark.sql.functions import col, first, lit
from pyspark.sql.types import *
from pyspark.databricks.sql.functions import h3_boundaryaswkb

# COMMAND ----------

catalog_name = "pilot"
bronze_schema_name = "bronze_test"
silver_schema_name = "test_silver"

tessellated_table = f"{catalog_name}.{bronze_schema_name}.countries_h3"
chip_table = f"{catalog_name}.{silver_schema_name}.esri_worldcountryboundaries_global_silver_chips"
final_countries_table = f"{catalog_name}.{silver_schema_name}.esri_worldcountryboundaries_global_silver2"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union chips in each country's H3 cells

# COMMAND ----------

# MAGIC %md
# MAGIC For a cleaner approach for combining chips within the same cell for a country, we first confirm some assumptions:
# MAGIC
# MAGIC 1. The core cells appear only once in the dataset
# MAGIC 2. A cell ID occurs for either core chips or non-core chips, but not both.
# MAGIC 3. Each geom_id is from one country. Indeed, geom_id was a hash of columns including country, but just checking.

# COMMAND ----------

max_duplicates_of_core_chips = spark.sql(f"""
    with core_cells as (
        select cellid from {tessellated_table}
        where core
    )
    select cellid from core_cells
    group by cellid
    having count(*) > 1
""").count()

assert max_duplicates_of_core_chips == 0, "Some core cell IDs occur twice"

cell_id_core_statuses = spark.sql(f"""
    select count(distinct core) from {tessellated_table}
    group by cellid
""").distinct().count()

assert cell_id_core_statuses == 1, "Some cell IDs occur both as core and non-core chips."

countries_per_geom_id = spark.sql(f"""
    select count(distinct country) from {tessellated_table}
    group by geom_id
""").distinct().count()

assert countries_per_geom_id == 1, "Some geom_ids include multiple countries."

# COMMAND ----------

# MAGIC %md
# MAGIC The "country_chips" table we create below includes just country names, H3 cell IDs, chip WKB, and the hasedh geom_id. So it is useful for fast joins that get the cells or chips covering a country.

# COMMAND ----------

(
    spark.table(tessellated_table)
    .select("country", "cellid", "chip", "geom_id", "core")
        # The key geom_id was a hash of country and other geometry-derived columns,
        # so this groupBy is within coutries
        .groupBy("geom_id", "country", "cellid", "core") 
        .agg( # Combine geometries together within a chip
            mos.st_union_agg(col("chip")).alias("chip_wkb"),
        )
        .withColumnRenamed("core", "chip_is_core")
        .withColumnRenamed("cellid", "grid_index")
        .write
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(chip_table)
)

# COMMAND ----------

# Liquid clustering
spark.sql(f"ALTER TABLE {chip_table} CLUSTER BY (grid_index, country)")

spark.sql(f"OPTIMIZE {chip_table}")

display(spark.sql(f"DESCRIBE DETAIL {chip_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish

# COMMAND ----------

# MAGIC %md
# MAGIC Create the final tessellated countries table by dropping columns used for our analysis adn joining the original country information with our streamlined chips.

# COMMAND ----------

analyis_columns_to_drop = ",".join([
  "geom_area",
  "am_cross",
  "num_pnts",
  "h3_res",
  "poly_id",
  "cellid",
  "chip",
  "core",
  "is_valid",
  "xmin", "xmax", "ymin", "ymax",
  "to_srid", "from_srid"
])

countries_tessellated = (
spark.table(tessellated_table)
.selectExpr(f"* except ({analyis_columns_to_drop})")
.dropDuplicates(["geom_id"])
.join(spark.table(chip_table).drop("country"), "geom_id", "left")
.write
.mode("overwrite")
.option("overwriteSchema", True)
.saveAsTable(final_countries_table)
)

# COMMAND ----------

# Liquid clustering
spark.sql(f"ALTER TABLE {final_countries_table} CLUSTER BY (grid_index, country)")

spark.sql(f"OPTIMIZE {final_countries_table}")

display(spark.sql(f"DESCRIBE DETAIL {final_countries_table}"))
