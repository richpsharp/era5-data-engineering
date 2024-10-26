# Databricks notebook source
# MAGIC %md
# MAGIC ## Checking if the silver table exists
# MAGIC
# MAGIC **If it does then do not run the rest of the code** 
# MAGIC
# MAGIC **If it does then run the code**

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
    dbutils.notebook.exit("The Delta table already exists.")
else:
    print(f"The Delta table '{table_name}' does not exist. Proceeding with the notebook execution.")


# COMMAND ----------

# MAGIC
# MAGIC %md # [1] Ingest Countries
# MAGIC
# MAGIC > Data has already been downloaded into `/Volumes/gwsc/countries/shapefile` [[1](https://e2-demo-field-eng.cloud.databricks.com/explore/data/volumes/gwsc/countries/shapefile?o=1444828305810485)], originally from ESRI World Country Boundaries [[2](https://hub.arcgis.com/datasets/esri::world-countries-generalized/explore?location=-0.319193%2C-167.469317%2C1.49)].
# MAGIC
# MAGIC Here is more information pertaining to the Esri world countries shapefiles. 
# MAGIC
# MAGIC 1. [World_Countries (FeatureServer) (arcgis.com)](https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/World_Countries/FeatureServer) 
# MAGIC 1. [Layer: World_Countries (ID:0) (arcgis.com)](https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/World_Countries/FeatureServer/0)
# MAGIC
# MAGIC This data presents some problems which we will seek to identify and mitigate prior to tessellating.
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

catalog_name = "era5-daily-data"
schema_name = "bronze_dev"
spark.catalog.setCurrentCatalog(catalog_name)
spark.catalog.setCurrentDatabase(schema_name)
# display(sql("show tables"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Shapefile Loading
# MAGIC
# MAGIC > Write records as Delta Lake, with initial `is_valid` check and adding a consistent `country_id`.

# COMMAND ----------

VOL_ZIP_DIR = "/Volumes/pilot/vector_files/country_boundaries/zip" 
DBFS_ZIP_DIR = "/tmp/countries"
VOL_SHP_DIR = "/Volumes/pilot/vector_files/country_boundaries"

os.environ['VOL_ZIP_DIR'] = VOL_ZIP_DIR
os.environ['DBFS_ZIP_DIR'] = DBFS_ZIP_DIR
os.environ['VOL_SHP_DIR'] = VOL_SHP_DIR

dbutils.fs.mkdirs(DBFS_ZIP_DIR)

# COMMAND ----------

ls -lh $VOL_SHP_DIR

# COMMAND ----------

# MAGIC %sh 
# MAGIC # -- uncomment to (re)run -- 
# MAGIC # rm -rf countries.shp.zip
# MAGIC if [ ! -f countries.shp.zip ]; then
# MAGIC   zip -rj countries.shp.zip $VOL_SHP_DIR
# MAGIC fi
# MAGIC
# MAGIC cp -n countries.shp.zip $VOL_ZIP_DIR
# MAGIC cp -n countries.shp.zip /dbfs$DBFS_ZIP_DIR
# MAGIC ls -lh /dbfs$DBFS_ZIP_DIR

# COMMAND ----------

# MAGIC %sh
# MAGIC ## -- uncomment to (re)run -- 
# MAGIC # unzip -l countries.shp.zip

# COMMAND ----------

# MAGIC %md
# MAGIC _Load shapefiles into spark, from [Vector Format Readers](https://databrickslabs.github.io/mosaic/api/vector-format-readers.html#spark-read-format-shapefile)._

# COMMAND ----------

# sql("drop table if exists countries_raw")
if not spark.catalog.tableExists(f"{schema_name}.countries_raw"):
  (
    spark.read.format("shapefile")
      .option("vsizip", "true")
      .option("layerNumber", "0")
      .option("asWKB", "true") 
    .load(DBFS_ZIP_DIR)
    .withColumn("is_valid", mos.st_isvalid("geom_0"))
    .selectExpr("xxhash64(country, countryaff, land_type, land_rank, shape_leng, shape_area, geom_0) as geom_id", "* except(geom_0_srid)", "cast(geom_0_srid as int) as from_srid")
    .write
      .mode("overwrite")
      .saveAsTable("countries_raw")
  )

df_raw = spark.read.table("countries_raw") 
print(f"count? {df_raw.count():,}")
# df_raw.display()
