# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Checking if the silver table exists
# MAGIC
# MAGIC **If it does then do not run the rest of the code**

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the path or table name for the Delta table
table_name = "`era5-daily-data`.silver_dev.esri_worldcountryboundaries_global_silver"

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
# MAGIC
# MAGIC ## [2] Validate Countries
# MAGIC
# MAGIC > This data presents some validity issues which we will seek to identify and mitigate prior to tessellating.
# MAGIC
# MAGIC ---
# MAGIC __Author:__ Michael Johns | __Last Modified:__ 12 APR, 2024 [Mosaic 0.4.1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Tips
# MAGIC
# MAGIC 1. Think about memory - cpu ratios, e.g. used [r5d.4xlarge](https://aws.amazon.com/ec2/instance-types/m6i/) (128GB , 16 cores) 
# MAGIC 2. spark session configs used
# MAGIC     * `spark.conf.set("spark.databricks.geo.st.enabled", True)` - turn on spatial sql
# MAGIC     * `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)` - tweak partition management
# MAGIC     * `spark.conf.set("spark.sql.shuffle.partitions", 1_024)` - default is 200
# MAGIC 3. Additional spark cluster config `spark.task.cpus 2` [or other multiple] can be used to help with ratio [not needed for this notebook]
# MAGIC 4. Use `repartition` to drive distributed execution
# MAGIC
# MAGIC _Note: Just because you hit "cancel" on execution, it doesn't mean that underlying resources are immediately free, so watch the [cluster metrics](https://docs.databricks.com/en/compute/cluster-metrics.html) for when actually released._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION # LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install databricks-mosaic==0.4.1 --quiet # <- geopandas, shapely, fiona deps

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
mos.enable_mosaic(spark)
mos.enable_gdal(spark)

# -- for execution --
from pyspark.databricks.sql import functions as dbf # <-- available on photon clusters
from pyspark.sql import functions as F
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import *

# -- others --
import os
# import pandas as pd

# COMMAND ----------

catalog_name = "`era5-daily-data`"
schema_name = "bronze_dev"
spark.catalog.setCurrentCatalog(catalog_name)
spark.catalog.setCurrentDatabase(schema_name)
# display(sql("show tables"))

# COMMAND ----------

df_raw = spark.read.table("countries_raw") 
print(f"count? {df_raw.count():,}")
# df_raw.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Geometries
# MAGIC
# MAGIC > There are some invalid geoms as well as some `SRID=0` vs `SRID=3857` (Web Mercator) that we will want to handle.

# COMMAND ----------

# MAGIC %md
# MAGIC __Which geoms are null? [None]__

# COMMAND ----------

# MAGIC %sql select * from countries_raw where geom_0 is null

# COMMAND ----------

# MAGIC %md
# MAGIC __Which geoms are invalid via `st_isvalid`? [37]__

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from countries_raw where is_valid = false

# COMMAND ----------

# MAGIC %sql select geom_id, country, land_type from countries_raw where is_valid = false

# COMMAND ----------

# MAGIC %md
# MAGIC __What are the SRIDs?__
# MAGIC
# MAGIC > Note: SRID 0 is in the data for a handful of empty POINTS representing "very small islands" [7]

# COMMAND ----------

# MAGIC %sql select distinct from_srid, count(1) as count from countries_raw group by from_srid

# COMMAND ----------

# MAGIC %sql select geom_id, country, land_type from countries_raw where from_srid = 0

# COMMAND ----------

# MAGIC %md
# MAGIC __How many empty geometries, using area as a measure? [7]__

# COMMAND ----------

# MAGIC %sql select geom_id, country, land_type from countries_raw where shape_area = 0 order by country

# COMMAND ----------

# MAGIC %md
# MAGIC __What does the area sizes look like?__
# MAGIC
# MAGIC > Start with the "as-provided" areas, then look at the calculated areas (they are essentially the same, with units in m^2). 
# MAGIC
# MAGIC _Note: 3857 is in projected meters which is way off from ground-truth meters as you move away from the equator._

# COMMAND ----------

# MAGIC %sql select country, shape_area, is_valid from countries_raw where order by shape_area desc

# COMMAND ----------

# MAGIC %sql select country, shape_area, st_area(geom_0) as calc_area, is_valid from countries_raw where order by shape_area desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Step-1] Transform All Geometries to 4326
# MAGIC
# MAGIC > Standardize geometries into `SRID=4326` (WGS84), Leaving out `SRID=0` small islands and Antartica, for now.
# MAGIC
# MAGIC __Note: H3 uses / expects `SRID=4326`.__

# COMMAND ----------

# sql("drop table if exists countries_4326")
if not spark.catalog.tableExists(f"{schema_name}.countries_4326"):
   (
     spark.read.table("countries_raw")
         .filter(F.expr("from_srid > 0"))           # <- skip the small islands with no area
         .filter(F.expr("country <> 'Antarctica'")) # <- skip Antarctica
         .withColumn(
            "geom_0", 
            mos.st_setsrid(mos.st_geomfromwkt("geom_0"), "from_srid")
         )
         .withColumn("to_srid", F.lit(4326))
         .withColumn(
            "geom_0", 
            mos.st_astext(mos.st_transform("geom_0", F.lit(4326)))
         )
         .write
         .mode("overwrite")
         .saveAsTable("countries_4326")
   )

df_4326 = spark.read.table("countries_4326") 
print(f"count? {df_4326.count():,}")
# df_4326.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Step-2] Fix Any invalid Geometries

# COMMAND ----------

# MAGIC %md
# MAGIC _Use a couple of UDFs from [Shapely Validate Example](https://github.com/databrickslabs/mosaic/blob/main/notebooks/examples/python/Validate/shapely_validate_udfs.ipynb)._

# COMMAND ----------

@udf(returnType=StringType())
def explain_wkt_validity(geom_wkt:str) -> str:
    """
    Add explanation of validity or invalidity
    """
    from shapely import wkt
    from shapely.validation import explain_validity

    _geom = wkt.loads(geom_wkt)
    return explain_validity(_geom)


@udf(returnType=StringType())
def make_wkt_valid(geom_wkt:str) -> str:
    """
    - test for wkt being valid
    - attempts to make valid
    - may have to change type, e.g. POLYGON to MULTIPOLYGON
     returns valid wkt
    """
    from shapely import wkt 
    from shapely.validation import make_valid

    _geom = wkt.loads(geom_wkt)
    if _geom.is_valid:
        return geom_wkt
    _geom_fix = make_valid(_geom)
    return _geom_fix.wkt

# COMMAND ----------

# MAGIC %md
# MAGIC ### [2.1] Explore `st_valid = False`
# MAGIC
# MAGIC > We find that all 37 invalids are ring self-intersections.

# COMMAND ----------

df_invalid = df_4326.filter(F.expr("is_valid = false"))
print(f"count? {df_invalid.count():,}")
#df_invalid.display() # <- only showing one due to size of geom

# COMMAND ----------

# MAGIC %md
# MAGIC _First, understand why the geometries are invalid with `explain_wkt_validity`._

# COMMAND ----------

# display(
#   df_invalid
#   .withColumn(
#     "explain_invalid", 
#     explain_wkt_validity("geom_0")
#   )
#   .select("geom_id", "country", "explain_invalid", "countryaff", "land_type", "shape_leng", "shape_area", "from_srid")
# )

# COMMAND ----------

df_new_valid = (
  df_invalid
    .withColumn("geom_0", make_wkt_valid("geom_0"))
    .withColumn("is_valid", mos.st_isvalid("geom_0"))
)

print(f"""count? {df_new_valid.count():,}""")
# display(df_new_valid.drop("geom_0")) # <- dropping geom_0 just for rendering

# COMMAND ----------

# MAGIC %md
# MAGIC ### [2.2] Apply `make_wkt_valid` [641]
# MAGIC
# MAGIC > Correct `is_valid=False` and write to table.
# MAGIC
# MAGIC __Note: this could also be done as a transactional merge.__

# COMMAND ----------

# sql("drop table if exists countries_valid")
if not spark.catalog.tableExists(f"{schema_name}.countries_valid"):
  (
    spark.read.table("countries_4326") 
      .withColumn(
        "geom_0",
        F.when(
          F.expr("is_valid = true"), col("geom_0")
        ).otherwise(
          make_wkt_valid("geom_0")
        )
      )
      .withColumn(
        "is_valid",
        mos.st_isvalid("geom_0")
      )
      .write
      .mode("overwrite")
      .saveAsTable("countries_valid")
  )

df_valid = spark.read.table("countries_valid") 
print(f"count? {df_valid.count():,}")
print(f"valid count? {df_valid.filter(F.expr('is_valid = true')).count():,}")
# df_valid.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Step-3] Flatten All MultiPolygons [~120K]
# MAGIC
# MAGIC > Since we are indexing a worldwide dataset, we want to take advantage of flattened multi-polygons to aid in distributed processing.
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC 1. Storing bounds for the geometry
# MAGIC 1. Identifying anti-meridian crossing
# MAGIC 1. Identifying numpoints for the flattened polygons
# MAGIC 1. Calc `geom_area` (units degrees)
# MAGIC 1. Generating `poly_id` for each polygon which is the combo of `geom_id` and `geom_0`

# COMMAND ----------

# sql("drop table if exists countries_flat")
if not spark.catalog.tableExists(f"{schema_name}.countries_flat"):
  (
    spark.read.table("countries_valid") 
      .withColumn(
        "geom_0",
        mos.flatten_polygons("geom_0")
      )
      .withColumn("xmin", F.expr("st_xmin(geom_0)"))
      .withColumn("ymin", F.expr("st_ymin(geom_0)"))
      .withColumn("xmax", F.expr("st_xmax(geom_0)"))
      .withColumn("ymax", F.expr("st_ymax(geom_0)"))
      .withColumn("geom_area", F.expr("st_area(geom_0)"))
      .withColumn(
        "am_cross", 
        (F.expr("xmin < -50") & F.expr("xmax > 50") &
        F.expr("xmin < 0") & F.expr("xmax > 0"))
      )
      .select(F.expr("xxhash64(geom_0, geom_id) as poly_id"), "*", mos.st_numpoints("geom_0").alias("num_pnts"))
      .write
      .mode("overwrite")
      .saveAsTable("countries_flat")
  )

df_flat = spark.read.table("countries_flat") 
print(f"count? {df_flat.count():,}")
# df_flat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## [Step-4] Any Anti-Meridian Crossings [0]
# MAGIC
# MAGIC > While the MultiPolygons have anti-meridian crossings (allowed), the individual polygons do not (not allowed, so all good).

# COMMAND ----------

print(f"count? {df_flat.filter(F.expr('am_cross = true')).count():,}")
# (
#   df_flat
#     .filter(F.expr("am_cross = true"))
#     .drop("geom_0") # <- for rendering
# ).display()
