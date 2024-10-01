# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %pip install databricks-mosaic antimeridian

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# -- mosaic --


import mosaic as mos
mos.enable_mosaic(spark)
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# -- for execution --
from pyspark.databricks.sql import functions as dbf # <-- available on photon clusters
from pyspark.sql import functions as F
from pyspark.sql.functions import col, first, lit
from pyspark.sql.functions import col, pandas_udf
from pyspark.databricks.sql.functions import h3_boundaryaswkb
from pyspark.sql.types import *


# -- others --
import os
import math




# COMMAND ----------

# MAGIC %md
# MAGIC ### df_tessellate

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

# COMMAND ----------


def df_tessellate(
  land_type, skip_ids=None, only_ids=None, overwrite=False, 
  geom_wkb=True, simp_tol=None, buff_rad=None, 
  area_thresh=25, point_thresh=25_000, p_fact=10,
  h3_res=4, do_display=True, display_limit=25, 
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
  print(f"...records? {df_filter.count()}")
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
        .withColumn("geom_0", mos.st_simplify(col("geom_0"), F.lit(simp_tol)))
    )
  elif buff_rad:
    df_filter = (
      df_filter
        .withColumn("geom_0", mos.st_buffer(col("geom_0"), F.lit(buf_rad)))
    )

  # - normal tessellate below thresh
  # - NOTE: area and point must be below thresh for normal handling
  n_cnt = df_filter.filter(F.expr(f"geom_area < {area_thresh}") & F.expr(f"num_pnts < {point_thresh}")).count()
  if n_cnt > 0:
    print(f"...handling normal tessellation, count? {n_cnt:,}")
    if geom_wkb:
      df_filter = (
        df_filter.withColumn("geom_0", mos.st_asbinary(col("geom_0")))
      )

    (
      df_filter      
        .filter(
          F.expr(f"geom_area < {area_thresh}") & # WAS |, WHICH SEEMS WRONG 
          F.expr(f"num_pnts < {point_thresh}")
        )
        .withColumn("h3_res", F.lit(h3_res))
        .select(mos.grid_tessellateexplode(col("geom_0"), col("h3_res")), F.expr("* except(geom_0)"))
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
      .withColumn("geom_0", mos.st_astext(col("geom_0")))
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
            .withColumn("chip", mos.st_asbinary(col("chip")))
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
# MAGIC ### Tessalte test

# COMMAND ----------

###########
## UNIT TEST #
##########

import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession, functions as F

# Mock function for dbfs complex operations and spatial functions
def mock_complex_write_udf(*args):
    return F.lit("complex_fuse_dir/geom_0_test")

def mock_complex_core_udf(*args):
    return F.lit(True)

def mock_h3_coverash3(*args):
    return F.lit("mock_h3_cell")

def mock_h3_boundaryaswkt(*args):
    return F.lit("mock_wkt_boundary")

def mock_complex_boundary_chip_udf(*args):
    return F.lit("mock_chip")

class TestDfTessellate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up the Spark session
        cls.spark = SparkSession.builder.master("local[1]").appName("df_tessellate_test").getOrCreate()
        cls.spark.catalog.setCurrentCatalog("era5-daily-data")
        cls.spark.catalog.setCurrentDatabase("bronze_dev")

        # Create the test dataframe
        cls.schema = ["poly_id", "land_type", "geom_area", "num_pnts", "geom_0"]
        cls.data = [
            (1, "Primary land", 10, 2000, "POINT (0 0)"),
            (2, "Medium island", 50, 10000, "POINT (0 0)"),
            (3, "Small island", 100, 30000, "POINT (0 0)"),
            (4, "Very small island", 200, 90000, "POINT (0 0)"),
        ]
        cls.create_test_dataframe = cls.spark.createDataFrame(cls.data, cls.schema)

    @patch("mosaic.st_simplify", lambda x, y: x)
    @patch("mosaic.st_buffer", lambda x, y: x)
    @patch("mosaic.st_asbinary", lambda x: x)
    @patch("mosaic.st_astext", lambda x: x)
    @patch("__main__.mock_complex_write_udf", mock_complex_write_udf)
    @patch("__main__.mock_complex_core_udf", mock_complex_core_udf)
    @patch("__main__.mock_h3_coverash3", mock_h3_coverash3)
    @patch("__main__.mock_h3_boundaryaswkt", mock_h3_boundaryaswkt)
    @patch("__main__.mock_complex_boundary_chip_udf", mock_complex_boundary_chip_udf)
    def test_df_tessellate(self):
        print("Test started")  # Print statement to ensure the test runs
        df = self.create_test_dataframe

        # Register the dataframe as a temporary view, as the function is filtering on a table
        df.createOrReplaceTempView("countries_flat")

        # Call the function under test
        df_tessellate(
            land_type="Primary land",
            overwrite=True,
            area_thresh=25,
            point_thresh=5000,
            h3_res=4,
            do_display=False
        )

        # Assertions: Check if the transformations are correct
        tessellated_df = self.spark.sql("SELECT * FROM main.default.countries_h3")
        
        # Example: check that rows have been inserted based on the tessellation logic
        self.assertGreater(tessellated_df.count(), 0, "No rows were tessellated")
        self.assertGreater(
            tessellated_df.filter(F.col("land_type") == "Primary land").count(), 0, "Primary land not tessellated correctly"
        )

        # Additional assertions based on the expected behavior
        self.assertIn("cellid", tessellated_df.columns, "cellid column missing in output")
        self.assertIn("core", tessellated_df.columns, "core column missing in output")
        self.assertIn("chip", tessellated_df.columns, "chip column missing in output")

        # Clean up
        self.spark.catalog.dropTempView("countries_flat")
        self.spark.catalog.dropTempView("countries_h3")

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestDfTessellate("test_df_tessellate"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()


