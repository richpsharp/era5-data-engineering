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

# MAGIC ## [3] Tessellate Countries
# MAGIC
# MAGIC > Tessellate previously cleaned up country polygons. __Note: Will generate at DLT pipeline that "just handles" shapefile to full tessellation.__
# MAGIC
# MAGIC ---
# MAGIC __Author:__ Michael Johns | __Last Modified:__ 23 APR, 2024 [Mosaic 0.4.1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Tips
# MAGIC
# MAGIC 1. Think about memory - cpu ratios, e.g. used [r5d.4xlarge](https://aws.amazon.com/ec2/instance-types/m6i/) (128GB , 16 cores)
# MAGIC 2. spark session configs used
# MAGIC     * `spark.conf.set("spark.databricks.geo.st.enabled", True)` - turn on spatial sql
# MAGIC     * `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)` - tweak partition management
# MAGIC     * `spark.conf.set("spark.sql.shuffle.partitions", 1_024)` - default is 200
# MAGIC 3. Additional spark cluster config `spark.task.cpus` can be used to help with ratio [must be done at cluster settings vs session]
# MAGIC     * Not needed in series due to addition of complex handling if polygon area > 25 degrees^2 or 25K points (mitigates the need)
# MAGIC 4. Use `repartition` to drive distributed execution
# MAGIC
# MAGIC _Note: Just because you hit "cancel" on execution, it doesn't mean that underlying resources are immediately free, so watch the [cluster metrics](https://docs.databricks.com/en/compute/cluster-metrics.html) for when actually released._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports + Setup

# COMMAND ----------

# MAGIC
# MAGIC %pip install numpy==1.26.4 ## please use this version of numpy ## DO NOT USE THE LATEST VERSION # LATEST VERSION WILL CAUSE NOTEBOOK TO CRASH
# MAGIC %pip install antimeridian databricks-mosaic==0.4.1 --quiet # <- geopandas, shapely, fiona deps

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
mos.enable_gdal(spark)

# -- for execution --
from pyspark.databricks.sql import functions as dbf # <-- available on photon clusters
from pyspark.sql import functions as F
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import *

# -- others --
import math
import os
# import pandas as pd

# COMMAND ----------

catalog_name = "pilot"
schema_name = "bronze_test"
spark.catalog.setCurrentCatalog(catalog_name)
spark.catalog.setCurrentDatabase(schema_name)
# display(sql("show tables"))

# COMMAND ----------

df_flat = spark.table("countries_flat")
print(f"count? {df_flat.count():,}")
# df_flat.display() # <- whole world countries as polygons

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tessellate @ `h3_res=5`

# COMMAND ----------

df_flat.groupBy("land_type").count().orderBy("count").display()

# COMMAND ----------

@udf(returnType=StringType())
def complex_write_udf(geom_wkt:str, geom_path_fuse:str) -> str:
  """
  Support metadata pattern to write a WKT to fuse path.
  - this is to avoid having to repeat the geometry in every row;
    in the case of larger geometries, this is the ONLY way
  - this would be done before tessellate, then just 
    the metadata would be included in the explode
  Returns the geom_path_fuse provided.
  """
  import os
  from pathlib import Path

  # - no op if already exists and more than 1MB
  fp = Path(geom_path_fuse)
  if fp.exists() and fp.stat().st_size > 1_000_000:
    # fp.unlink(missing_ok=True) # <- testing
    return geom_path_fuse
  
  # - make dirs and write the WKT
  fp.parents[0].mkdir(parents=True, exist_ok=True)
  with fp.open(mode="w", encoding="utf-8") as f:
    f.write(geom_wkt)
  return geom_path_fuse

# COMMAND ----------

@udf(returnType=BooleanType())
def complex_core_udf(geom_path_fuse:str, cellid_wkt:str) -> bool:
  """
  Support metadata pattern using a fuse path to a geometry WKT.
  - determine whether a cellid_wkt is core to the geom or not
  Return true if core; otherwise, false.
  """
  from shapely import wkt
  
  g1 = None
  with open(geom_path_fuse, mode="r", encoding="utf-8") as f:
    g1 = wkt.load(f)         # <- file
  g2 = wkt.loads(cellid_wkt) # <- string
  return g1.contains(g2)  

# COMMAND ----------

def antimeridian_safe_chip(geom_wkt, cellid_wkt, debug_level=0):
  """
  For cellids located near the antimeridian, 
  boundary chip generation may have issues. 
   - Assumes geom_wkt does not cross AM; 
     you would have previously handled that, if so.
   - Steps:
     [1] Initially tries to generate the boundary chip
     [2] if that fails and the cellid is near AM...
         (a) splits the cellid boundary at AM
         (b) uses the relevant side of AM for the chipping
     [3] if that fails and cellid is not near AM, returns null

  Returns AM safe `chip` as would be returned by h3_tessellateaswkb.
  """
  import antimeridian
  import shapely
  from shapely import wkt
  
  # [1] as shapely objects
  #    - should not have issues here
  g1 = wkt.loads(geom_wkt)
  g2 = wkt.loads(cellid_wkt) # <- string
  
  # [2] try intersection
  #    - return if successful (most common)
  try:
    return shapely.intersection(g1,g2).wkt 
  except:
    pass # <- keep going on initial exception

  # [3] get bounds for both
  (xmin1, _, xmax1, _) = g1.bounds
  (xmin2, _, xmax2, _) = g2.bounds
  (debug_level > 0) and print(f"xmin1? {xmin1}, xmax1? {xmax1}")
  (debug_level > 0) and print(f"xmin2? {xmin2}, xmax2? {xmax2}")

   # [4] geom in western hemisphere only
  if xmin1 >= -180 and xmax1 <= 0:
    if xmin2 >= -180 and xmax2 <= 0:
      (debug_level > 0) and print(f"west -> AM split won't help...")
      return None # <- AM split won't help
    try:
      g2_split = antimeridian.fix_polygon(g2, force_north_pole=False, force_south_pole=False, fix_winding=True)
      # - only intereseted in multipolygons for this
      if isinstance(g2_split, shapely.geometry.MultiPolygon):
        for _g2_chip in list(g2_split.geoms):
          (_xmin, _, _xmax1, _) = _g2_chip.bounds
          if _xmin < 0:
            try:
              inter_wkt = shapely.intersection(g1,_g2_chip).wkt # <- western chip
              (debug_level > 0) and print(f"west -> had a success (returning)...")
              return inter_wkt
            except:
              pass
      (debug_level > 0) and print(f"west -> no success...")
      return None # <- no successes
    except:
      (debug_level > 0) and print(f"west -> split exception...")
      return None # <- exception 
  # [5] geom in eastern hemisphere only
  elif xmin1 >= 0 and xmax1 < 180:
    if xmin2 >= 0 and xmax2 < 180:
      (debug_level > 0) and print(f"east -> AM split won't help...")
      return None # <- AM split won't help
    try:
      g2_split = antimeridian.fix_polygon(g2, force_north_pole=False, force_south_pole=False, fix_winding=True)
      # - only intereseted in multipolygons for this
      if isinstance(g2_split, shapely.geometry.MultiPolygon):
        for _g2_chip in list(g2_split.geoms):
          (_xmin, _, _xmax1, _) = _g2_chip.bounds
          if _xmin > 0:
            try:
              inter_wkt = shapely.intersection(g1,_g2_chip).wkt # <- western chip
              (debug_level > 0) and print(f"west -> had a success (returning)...")
              return inter_wkt
            except:
              pass
      (debug_level > 0) and print(f"east -> no success...")
      return None # <- no successes
    except:
      (debug_level > 0) and print(f"east -> split exception...")
      return None # <- exception 
  else:
    (debug_level > 0) and print(f"geom crosses AM / PM  (not the focus) -> split exception...")
    return None # <- geom crosses PM or AM (not the focus)


@udf(returnType=StringType())
def complex_boundary_chip_udf(geom_path_fuse:str, cellid_wkt:str) -> str:
  """
  Support metadata pattern using a fuse path to a geometry WKT.
  - determine the intersection of the cellid boundary with the geometry
  - this is only intended for boundary cellids; otherwise, wasted work
  - this will return none for any exception, will have to verify and fix afterwards;
    e.g. Topology Exception
  - Exception handling only on fully Eastern or Western Hemisphere geom_wkts; 
    returns None if Prime Meridian (0) or Anti Meridian (-180) is crossed,
    if the initial attempt at intersection failed (prior to splitting), that is.
  Returns WKT chip of the intersection.
  """
  import shapely
  from shapely import wkt
  
  geom_wkt = None
  with open(geom_path_fuse, mode="r", encoding="utf-8") as f:
    geom_wkt = f.read() # <- just want the string
  
  return antimeridian_safe_chip(geom_wkt, cellid_wkt)

# COMMAND ----------

def df_tessellate(
  land_type, skip_ids=None, only_ids=None, overwrite=False, 
  geom_wkb=True, simp_tol=None, buff_rad=None, 
  area_thresh=25, point_thresh=25_000, p_fact=10,
  h3_res=5, do_display=True, display_limit=25, 
  complex_fuse_dir='/dbfs/tmp/esri/countries'
):
  """
  [1] This makes a lot of assumptions, not for generic use as-is.
  [2] 'skip' and 'only' ids are `poly_id`.
  [3] This can handle the mitigations (st_simplify or st_buffer), e.g. combined with only_ids.
  [4] if area or point thresh met, use a complex strategy for tessellation.
  [5] partition factor for repartition after coveras in complex handling, e.g. ceil(c_cnt / p_fact) 
      default for p_fact is 10
  [6] complex fuse dir defaults to a tmp location, will be generated, not cleaned up automatically
  [7] repartitions are as follows: 
      - 'Large island'	17
      - 'Medium island'	473
      - 'Primary land'	1151
      - 'Small island'	35084
      - 'Very small island'	82944
  """
  reparts = {
    'Large island':	24,
    'Medium island': 64,
    'Primary land':	1_200,
    'Small island': 1_024,
    'Very small island': 1_024
  }

  if overwrite:
    print("...overwrite=True")
    sql("drop table if exists countries_h3")  

  df_filter = (
    spark.table("countries_flat")
      .filter(
        F.contains("land_type", F.lit(land_type))
      )
  )
  if skip_ids:
    df_filter = (
      df_filter.filter(col("poly_id").isin(*skip_ids) == False)
        .repartition(reparts[land_type] - len(skip_ids), "poly_id")
    )
  elif only_ids:
    df_filter = (
      df_filter.filter(col("poly_id").isin(*only_ids))
        .repartition(len(only_ids), "poly_id")
    )
  else:
    df_filter = df_filter.repartition(reparts[land_type], "poly_id")
  
  if simp_tol:
    df_filter = (
      df_filter
        .withColumn("geom_0", mos.st_simplify("geom_0", F.lit(simp_tol)))
    )
  elif buff_rad:
    df_filter = (
      df_filter
        .withColumn("geom_0", mos.st_buffer("geom_0", F.lit(buf_rad)))
    )

  # - normal tessellate below thresh
  # - NOTE: area and point must be below thresh for normal handling
  n_cnt = df_filter.filter(F.expr(f"geom_area < {area_thresh}") & F.expr(f"num_pnts < {point_thresh}")).count()
  if n_cnt > 0:
    print(f"...handling normal tessellation, count? {n_cnt:,}")

    if geom_wkb:
      df_filter = (
        df_filter.withColumn("geom_0", mos.st_asbinary("geom_0"))
      )

    (
      df_filter      
        .filter(
          F.expr(f"geom_area < {area_thresh}") & # WAS |, WHICH SEEMS WRONG 
          F.expr(f"num_pnts < {point_thresh}")
        )
        .withColumn("h3_res", F.lit(h3_res))
        .select(mos.grid_tessellateexplode("geom_0", "h3_res"), F.expr("* except(geom_0)"))
        # - adjust to product naming
        .selectExpr(
          "index.index_id as cellid", 
          "index.is_core as core",
          "index.wkb as chip",
          "* except(index)"
        ) 
        .write
          .mode("append")
          .saveAsTable("countries_h3")
    )

  # - complex tessellate above thresh
  # - NOTE: either area or points over thresh triggers 
  # - go ahead and convert geom_0 to wkt
  df_geom = (df_filter
      .filter(
        F.expr(f"geom_area >= {area_thresh}") | 
        F.expr(f"num_pnts >= {point_thresh}"))
      .withColumn("geom_0", mos.st_astext("geom_0"))
  )
  t_cnt = df_geom.count()
  if t_cnt > 0:
    print(f"...handling complex tessellation, count? {t_cnt:,}")

    # - write geom_0 
    df_meta = None
    try: 
      df_meta = (
        df_geom
          .select(
            "poly_id", 
            complex_write_udf(
              "geom_0",
              F.concat(F.lit(complex_fuse_dir), F.lit("/"), col("poly_id"))
              .alias("geom_path_fuse")
            )
          )
        .cache()
      )
      m_cnt = df_meta.count()
      print(f"\t...writing geom_0 to complex_fuse_dir '{complex_fuse_dir}', count? {m_cnt:,}")


      # - remove geom_0 from the explode
      df_h3 = (
        df_geom      
          .withColumn(
            "geom_path_fuse", 
            F.concat(F.lit(complex_fuse_dir),F.lit("/"), col("poly_id"))
          )
          .withColumn("h3_res", F.lit(h3_res))
          .select(
            F.explode(dbf.h3_coverash3("geom_0", "h3_res")).alias("cellid"),
            F.expr("* except(geom_0)") # <- geom_0 now at 'geom_fuse_path'
          )
      )

      # - emit the `cellid` (row) count after explode 
      #   used for repartitioning
      c_cnt = df_h3.count()
      repart_n = math.ceil(c_cnt / p_fact)
      print(f"\t...coveras rows? {c_cnt:,}, repart val? {repart_n:,}")

      # - handle complex chipping
      (
        df_h3
          .repartition(repart_n, "cellid")
            .withColumn(
              "cellid_wkt",
              dbf.h3_boundaryaswkt("cellid")
            )
            .select(
              "cellid",
              complex_core_udf("geom_path_fuse","cellid_wkt").alias("core"),
              F.expr("* except(cellid)")
            )
            .select(
              "cellid",
              "core",
              F
                .when(F.expr("core = true"), col("cellid_wkt"))
                .otherwise(complex_boundary_chip_udf("geom_path_fuse","cellid_wkt"))
              .alias("chip"),
              F.expr("* except(geom_path_fuse, cellid, core, cellid_wkt)")
            )
            .withColumn("chip", mos.st_asbinary("chip"))
          .write
            .mode("append")
            .saveAsTable("countries_h3")
      )
    finally:
      if df_meta:
        df_meta.unpersist() # <- uncache

  if do_display:
    _df = spark.table("countries_h3").filter(F.expr(f"land_type = '{land_type}'"))
    print(f"""land type '{land_type}' count? {_df.count():,}""")
    _df.limit(display_limit).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Large Islands [17 Polys -> 290 Chips]
# MAGIC
# MAGIC > Note: This has 16 normal processing and 1 complex in Indonesia (`poly_id=-8381902466908757764` > 25K points)

# COMMAND ----------

df_tessellate('Large island', overwrite=True, do_display=False) # <- NOTICE OVERWRITE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medium Islands [473 Polys -> 1,955 Chips]
# MAGIC
# MAGIC > All normal handling.

# COMMAND ----------

df_tessellate('Medium island', overwrite=False, do_display=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small Islands [35,084 Polys -> 40,655 Chips]
# MAGIC
# MAGIC > All normal handling.

# COMMAND ----------

df_tessellate('Small island', overwrite=False, do_display=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Very Small Islands [82,944 Polys -> 84,350 Chips]
# MAGIC
# MAGIC > All normal handling.

# COMMAND ----------

df_tessellate('Very small island', overwrite=False, do_display=False)

# COMMAND ----------

# MAGIC %md
# MAGIC  Before moving to Primary Land, lets add liquid clustering and optimize (will optimize the next section as well).

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- liquid clustering
# MAGIC ALTER TABLE countries_h3 CLUSTER BY (cellid);
# MAGIC OPTIMIZE countries_h3;
# MAGIC DESCRIBE DETAIL countries_h3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primary Land [1,151 Polys -> 119,959 Chips]
# MAGIC
# MAGIC > This has 1,061 normal processing polygons and 90 complex processing.

# COMMAND ----------

df_tessellate('Primary land', overwrite=False, do_display=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalize

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- liquid clustering
# MAGIC ALTER TABLE countries_h3 CLUSTER BY (cellid);
# MAGIC OPTIMIZE countries_h3;
# MAGIC DESCRIBE DETAIL countries_h3;

# COMMAND ----------

(
  spark.table("countries_h3")
    .groupBy("land_type")
    .count()
  .withColumn("count", F.format_number("count",0))
  .orderBy("land_type")
).display()
