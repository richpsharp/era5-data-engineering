# Databricks notebook source
from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Checking if the silver table exists
# MAGIC
# MAGIC **If it does then do not run the rest of the code**

# COMMAND ----------

if workspace_url == dev_workspace_url: 

    
    # Define the path or table name for the Delta table
    table_name = "`era5-daily-data`.silver_dev.esri_worldcountryboundaries_global_silver"

    # Check if the Delta table exists
    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"The Delta table '{table_name}' already exists. Skipping the rest of the notebook.")
        ## Skip the rest of the notebook
        dbutils.notebook.exit("The Delta table already exists.")
    else:
        print(f"The Delta table '{table_name}' does not exist. Proceeding with the notebook execution.")

elif workspace_url == staging_workspace_url: 

     # Define the path or table name for the Delta table
    table_name = "`era5-daily-data`.silver_staging.esri_worldcountryboundaries_global_silver"

    # Check if the Delta table exists
    if spark._jsparkSession.catalog().tableExists(table_name):
        print(f"The Delta table '{table_name}' already exists. Skipping the rest of the notebook.")
        ## Skip the rest of the notebook
        dbutils.notebook.exit("The Delta table already exists.")
    else:
        print(f"The Delta table '{table_name}' does not exist. Proceeding with the notebook execution.")

else: 
    print("This notebook is not intended to be run outside of dev or staging.")




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## [4] Viz Countries
# MAGIC
# MAGIC > Visualize some of the tessellated countries, using keplergl.
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
mos.enable_mosaic(spark)
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

from pyspark.sql import SparkSession



# Get the current workspace URL
workspace_url = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl", None)

# Dev workspace URL
dev_workspace_url = "dbc-ad3d47af-affb.cloud.databricks.com"

# Staging workspace URL
staging_workspace_url = "dbc-59ffb06d-e490.cloud.databricks.com"

# COMMAND ----------

if workspace_url == dev_workspace_url:
  catalog_name = "era5-daily-data"
  schema_name = "bronze_dev"
  spark.catalog.setCurrentCatalog(catalog_name)
  spark.catalog.setCurrentDatabase(schema_name)
  # display(sql("show tables"))
elif workspace_url == staging_workspace_url:
  catalog_name = "era5-daily-data"
  schema_name = "bronze_staging"
  spark.catalog.setCurrentCatalog(catalog_name)
  spark.catalog.setCurrentDatabase(schema_name)
else: 
  print("code is not designed to work in this workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC __Viz helper function `render_h3_chips`__
# MAGIC
# MAGIC > This uses Mosaic (instead of product) to stay with DBR 13.3 LTS, can additionally call `map_render` and `map_render_dfMapItems` functions. 

# COMMAND ----------

use_kepler = True
try:
  print(f"use keperl? {use_kepler}")

  if not use_kepler:
    print("customer disabled helpers...")
    raise Exception("customer disabled helpers...")

  # -- setup helper functions
  from dataclasses import dataclass
  from dataclasses import field
  from enum import Enum
  from keplergl import KeplerGl
  import math
  from pyspark.sql import DataFrame
  import re
  from typing import List

  kepler_height=800
  kepler_width=1200


  def display_kepler(kmap:KeplerGl, height:int=kepler_height, width:int=kepler_width) -> None:
    """
    Convenience function to render map in kepler.gl
    """
    decoded = (
        kmap._repr_html_()
        .decode("utf-8")
        .replace(".height||400", f".height||{height}")
        .replace(".width||400", f".width||{width}")
    )
    ga_script_redacted = re.sub(
        r"\<script\>\(function\(i,s,o,g,r,a,m\).*?GoogleAnalyticsObject.*?(\<\/script\>)",
        "",
        decoded,
        flags=re.DOTALL,
    )
    async_script_redacted = re.sub(
        r"s\.a\.createElement\(\"script\",\{async.*?\}\),",
        "",
        ga_script_redacted,
        flags=re.DOTALL,
    )
    displayHTML(async_script_redacted)


  class RENDER_TYPE(Enum):
    """Specify expected type of a 'render_col' in DFMapItem."""
    GEOMETRY = 100
    H3_INT = 200
    H3_STR = 300


  class GEO_FORMAT(Enum):
    """Specify expected type of a 'render_col' in DFMapItem [when RENDER_TYPE = GEOMETRY]."""
    WKT = 1
    WKB = 2
    GEOJSON = 3
    EWKB = 4

  @dataclass
  class DFMapItem:
    """Class for holding some properties for rendering a map."""
    df:DataFrame
    render_col:str
    render_type:RENDER_TYPE
    geo_format:GEO_FORMAT=None
    layer_name:str=None
    zoom_calc_sample_limit:int=None
    exclude_cols:list=field(default_factory=list)


  @dataclass
  class ZoomInfo:
    map_x:float
    map_y:float
    map_zoom:float

  default_ZoomInfo = ZoomInfo(0.0, 0.0, 3.0)


  def calc_ZoomInfo(dfMapItem:DFMapItem, debug_level:int=0) -> ZoomInfo:
    """
    Example output of debug_level=1
    {'xmin': -100.5, 'ymin': 50.05, 'xmax': -100.25, 'ymax': 50.5, 'centroid_x': -100.375, 'centroid_y': 50.275, 'pnt_sw': 'POINT(-100.5 50.05)', 'pnt_nw': 'POINT(-100.5 50.5)', 'pnt_se': 'POINT(-100.25 50.05)', 'pnt_ne': 'POINT(-100.25 50.5)',  'width_meters': 17905.33401827115, 'height_meters': 50055.461462782696, 'max_meters': 50055.461462782696, 'zoom': 9.5} 
    """
    # - handle zoom sample
    df_samp = dfMapItem.df
    samp_limit = dfMapItem.zoom_calc_sample_limit
    if samp_limit is not None:
      cnt = df_samp.count()
      if samp_limit < cnt:
        df_samp = (
          df_samp
            .dropna(dfMapItem.render_col)
            .sample(float(samp_limit)/float(cnt))
            .limit(samp_limit)
        )

    # - handle h3
    geom_col = dfMapItem.render_col
    if dfMapItem.render_type in [RENDER_TYPE.H3_INT,RENDER_TYPE.H3_STR]:
      geom_col = "h3_geom"
      df_samp = df_samp.withColumn(geom_col, F.expr(f"h3_boundaryaswkb({dfMapItem.render_col})"))

    # standardize to SRID=4326
    if dfMapItem.geo_format is not None:
      from_str = None
      if dfMapItem.geo_format == GEO_FORMAT.WKT:
        from_str='wkt'
      elif dfMapItem.geo_format == GEO_FORMAT.WKB:
        from_str='wkb'
      elif dfMapItem.geo_format == GEO_FORMAT.GEOJSON:
        from_str='geojson'
      elif dfMapItem.geo_format == GEO_FORMAT.EWKB:
        from_str='ewkb'
      # ... only do the operation if from_clause identified
      if from_str is not None:
        srid = df_samp.select(F.expr(f"st_srid(st_geomfrom{from_str}({geom_col}))")).first()[0]
        if srid is not None and srid > 0 and srid != 4326:
          df_samp = (
            df_samp
              .selectExpr(
                f"st_asbinary(st_transform(st_geomfrom{from_str}({geom_col}, 4326))) as {geom_col}", 
                f"* except({geom_col})"
              )
          )

    d = (
      df_samp
        # - xy min/max
        .select( 
          F.expr(f"st_xmin({geom_col}) as xmin"), 
          F.expr(f"st_ymin({geom_col}) as ymin"),
          F.expr(f"st_xmax({geom_col}) as xmax"),
          F.expr(f"st_ymax({geom_col}) as ymax")
        )
      .groupBy()
        .agg(
          F.min("xmin").alias("xmin"),
          F.min("ymin").alias("ymin"),
          F.max("xmax").alias("xmax"),
          F.max("ymax").alias("ymax")
        )
        # - centroid xy ranges
        .withColumn("centroid_x", F.expr("(xmin + xmax) / 2.0"))
        .withColumn("centroid_y", F.expr("(ymin + ymax) / 2.0"))  
        .withColumn("pnt_sw", F.expr("st_astext(st_point(xmin,ymin))"))
        .withColumn("pnt_nw", F.expr("st_astext(st_point(xmin,ymax))"))
        .withColumn("pnt_se", F.expr("st_astext(st_point(xmax,ymin))"))
        .withColumn("pnt_ne", F.expr("st_astext(st_point(xmax,ymax))"))
        .withColumn(
          "width_meters", 
          F.expr("st_haversine(xmin, ymin, xmax, ymin) * 1000") # <- sw, se (km to m)
        )
        .withColumn(
          "height_meters", 
          F.expr("st_haversine(xmin, ymin, xmin, ymax) * 1000") # <- sw, nw (km to m)
        )
        .withColumn(
          "max_meters", 
          F
            .when(F.expr("width_meters >= height_meters"), F.col("width_meters"))
            .otherwise(F.col("height_meters"))
        )
        # - zoom
        # https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Resolution_and_Scale
        # 1cm = ~.4in
        # assume 16cm = ~6in (height of viewport)
        # but mapbox tiles are 512px instead of 256px, so divide by 2 [h=8 tiles]
        .withColumn(
          "zoom",
          F
            .when(F.expr("max_meters < 21.2 * 8"), F.lit(18))
            .when(F.expr("max_meters < 42.3 * 8"), F.lit(17))
            .when(F.expr("max_meters < 84.6 * 8"), F.lit(16))
            .when(F.expr("max_meters < 169 * 8"), F.lit(15))
            .when(F.expr("max_meters < 339 * 8"), F.lit(14))
            .when(F.expr("max_meters < 677 * 8"), F.lit(13))
            .when(F.expr("max_meters < 1.35 * 1000 * 8"), F.lit(12))
            .when(F.expr("max_meters < 2.7  * 1000 * 8"), F.lit(11))
            .when(F.expr("max_meters < 5.4  * 1000 * 8"), F.lit(10))
            .when(F.expr("max_meters < 10.8 * 1000 * 8"), F.lit(9))
            .when(F.expr("max_meters < 21.7 * 1000 * 8"), F.lit(8))
            .when(F.expr("max_meters < 43.3 * 1000 * 8"), F.lit(7))
            .when(F.expr("max_meters < 86.7 * 1000 * 8"), F.lit(6))
            .when(F.expr("max_meters < 173  * 1000 * 8"), F.lit(5))
            .when(F.expr("max_meters < 347  * 1000 * 8"), F.lit(4))
            .when(F.expr("max_meters < 693  * 1000 * 8"), F.lit(3))
            .when(F.expr("max_meters < 1387 * 1000 * 8"), F.lit(2))
            .when(F.expr("max_meters < 2773 * 1000 * 8"), F.lit(1))
            .otherwise(F.lit(0))
        )
    ).first().asDict()

    (debug_level > 0) and print(d,"\n")
    return ZoomInfo(d['centroid_x'], d['centroid_y'], d['zoom'])


  def map_render_dfMapItems(*dfMapItems:List[DFMapItem], 
               override_ZoomInfo:ZoomInfo=None,
               kepler_map_style:str='dark',
               debug_level:int=0) ->None:
    """
    Calls `display_kepler` using conventions.
    - Calculates center lat/lon and zoom level [based on first layer passed], 
      if override ZoomInfo not specified
    - Renders one or more passed Spark DFMapItems,
      each will be a separate layer
    - Must specify render col and RENDER_TYPE
    - Can use specified layer name in DFMapItem;
      otherwise, will be generated
    - Can specify a render sample limit in DFMapItem
    - Can specify a zoom calc sample limit in DFMapItem;
      otherwise it will be all
    """

    layers = {}
    zoomInfo = default_ZoomInfo
    if override_ZoomInfo is not None:
      zoomInfo = override_ZoomInfo

    for layer_num, dfMapItem in enumerate(dfMapItems):
      # - zoom info [first layer]
      if layer_num == 0 and override_ZoomInfo is None:
        zoomInfo = calc_ZoomInfo(dfMapItem, debug_level=debug_level)

      # - layer name
      layer_name = dfMapItem.layer_name
      if layer_name is None:
        layer_name = f"layer_{layer_num}"

      # - data
      if dfMapItem.render_type in [RENDER_TYPE.GEOMETRY]:
        # handle binary serialization
        geo_format = dfMapItem.geo_format
        if geo_format is not None and geo_format in [GEO_FORMAT.WKB, GEO_FORMAT.EWKB]:
          layers[layer_name] = (
            dfMapItem
              .df
                .drop(*dfMapItem.exclude_cols)
              .toPandas()
                .to_csv(None, index=False)
          )
        else:
          layers[layer_name] = (
            dfMapItem
              .df
                .drop(*dfMapItem.exclude_cols)
              .toPandas()
          )
      elif dfMapItem.render_type in [RENDER_TYPE.H3_STR]:
          layers[layer_name] = (
            dfMapItem
              .df
                .drop(*dfMapItem.exclude_cols)
              .toPandas()
          )
      elif dfMapItem.render_type == RENDER_TYPE.H3_INT:
        layers[layer_name] = (
          dfMapItem
            .df
              .selectExpr(
                f"h3_h3tostring({dfMapItem.render_col}) as {dfMapItem.render_col}", 
                f"* except({dfMapItem.render_col})"
              )
              .drop(*dfMapItem.exclude_cols)
            .toPandas()
        )
      
    return display_kepler(
      KeplerGl(
        config={ 
          'version': 'v1', 
          'mapState': {
            'longitude': zoomInfo.map_x, 
            'latitude': zoomInfo.map_y, 
            'zoom': zoomInfo.map_zoom
          }, 
          'mapStyle': {'styleType': kepler_map_style},
          'options': {'readOnly': False, 'centerMap': True}
        },
        data=layers,
        show_docs=False,
      )
    )


  def map_render(df:DataFrame, geom_col:str, geo_format:GEO_FORMAT=None, exclude_cols:list=[], override_ZoomInfo:ZoomInfo=None, kepler_map_style='dark', debug_level:int=0)   ->None:
    """
    Render a Spark Dataframe, using geometry col for center and zoom,
    if overrides not specified. 
    """
    map_render_dfMapItems(DFMapItem(df, geom_col, RENDER_TYPE.GEOMETRY, geo_format=geo_format, exclude_cols=exclude_cols), 
               override_ZoomInfo=override_ZoomInfo, 
               kepler_map_style=kepler_map_style,
               debug_level=debug_level)

  print("---")
  print("def map_render(df:DataFrame, geom_col:str)")
  print("def map_render_dfMapItems(*dfMapItems:List[DFMapItem])")

except Exception:
  print("... `map_render` functions not available.")
  pass

# COMMAND ----------

def render_h3_chips(df, country, land_type='Primary land', do_details=False):
  """
  Convenience for rendering both h3 cells and the chips for a country
  """
  df_c = df.filter(F.expr(f"country = '{country}'"))

  if land_type:
    df_c = df_c.filter(F.expr(f"land_type = '{land_type}'"))

  if do_details:
    print(f"::: {country} :::")
    print(f"count? {df_c.count():,}")
    print(f"distinct h3? {df_c.select('cellid').distinct().count():,}")
    df_c.display()
  else:
    map_render_dfMapItems(
      DFMapItem(df_c.select('chip'), "chip", RENDER_TYPE.GEOMETRY, geo_format=GEO_FORMAT.WKT),
      DFMapItem(df_c.select('cellid').distinct(), "cellid", RENDER_TYPE.H3_INT)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC __Initial `df` and counts__

# COMMAND ----------

df = (
  spark.table("countries_h3")
    .selectExpr(
      "* except(land_rank, shape_leng, shape_area, is_valid, from_srid, to_srid, xmin, ymin, xmax, ymax, am_cross, h3_res)"
    )
    .withColumn("chip", mos.st_astext("chip"))
)

(
  df
    .groupBy("land_type")
    .count()
  .withColumn("count", F.format_number("count",0))
  .orderBy("land_type")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render

# COMMAND ----------

# MAGIC %md
# MAGIC ### USA

# COMMAND ----------

render_h3_chips(df, "United States", do_details=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Norway

# COMMAND ----------

render_h3_chips(df, "Norway", do_details=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fiji
# MAGIC
# MAGIC __Notes:__
# MAGIC
# MAGIC 1. Fiji has geometries on either side of the anti-meridian
# MAGIC 1. Because of (1), the function zooms to longitude 0 given the anti-meridian crossing (average of -180 and 180 is 0)
# MAGIC     * so have to move map over to 180th meridian
# MAGIC     * this is just some standing example code I am using to help with rendering, not that fancy
# MAGIC 1. Also, you have to wiggle on either side of the anti-meridian to change hemispheres and see
# MAGIC 1. Some of the H3 cells will not render properly given the anti-meridian crossing
# MAGIC 1. All the actual geometry chips render as expected (none cross the anti-meridian)

# COMMAND ----------

render_h3_chips(df, "Fiji", land_type=None, do_details=False)

# COMMAND ----------


