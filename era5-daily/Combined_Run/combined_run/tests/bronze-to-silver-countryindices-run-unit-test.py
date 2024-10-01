# Databricks notebook source
# MAGIC %pip install databricks-mosaic antimeridian
# MAGIC %pip install pytest

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



import unittest
from pyspark.sql import SparkSession




# COMMAND ----------


############
## DEFINITION #######
#############

@udf(returnType=StringType())
def explain_wkt_validity(geom_wkt: str) -> str:
    """
    Add explanation of validity or invalidity
    """
    from shapely import wkt
    from shapely.validation import explain_validity

    _geom = wkt.loads(geom_wkt)
    return explain_validity(_geom)




# COMMAND ----------


########
## UNIT TEST
########

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


class TestExplainWktValidity(unittest.TestCase):

    def setUp(self):
        self.invalid_wkt = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"  # Self-intersecting polygon
        self.valid_wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"  # Valid polygon

    def test_invalid_wkt(self):
        df = spark.createDataFrame([(self.invalid_wkt,)], ["geom_wkt"])
        result = df.select(explain_wkt_validity("geom_wkt").alias("explanation")).collect()
        self.assertTrue("Self-intersection" in result[0]["explanation"])

    def test_valid_wkt(self):
        df = spark.createDataFrame([(self.valid_wkt,)], ["geom_wkt"])
        result = df.select(explain_wkt_validity("geom_wkt").alias("explanation")).collect()
        self.assertEqual(result[0]["explanation"], "Valid Geometry")

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestExplainWktValidity("test_invalid_wkt"))
    suite.addTest(TestExplainWktValidity("test_valid_wkt"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()


# COMMAND ----------

##########
### DEFINITION
##############


import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def make_wkt_valid(geom_wkt: str) -> str:
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

##########
## UNIT TEST ##
########

import unittest
from pyspark.sql import SparkSession

class TestMakeWktValid(unittest.TestCase):

    def setUp(self):
        self.invalid_wkt = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"  # Self-intersecting polygon
        self.valid_wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"  # Valid polygon

    def test_invalid_wkt(self):
        df = spark.createDataFrame([(self.invalid_wkt,)], ["geom_wkt"])
        result = df.select(make_wkt_valid("geom_wkt").alias("fixed_wkt")).collect()
        fixed_geom = result[0]["fixed_wkt"]
        self.assertNotEqual(fixed_geom, self.invalid_wkt)
        self.assertTrue(fixed_geom.startswith("MULTIPOLYGON") or fixed_geom.startswith("POLYGON"))

    def test_valid_wkt(self):
        df = spark.createDataFrame([(self.valid_wkt,)], ["geom_wkt"])
        result = df.select(make_wkt_valid("geom_wkt").alias("fixed_wkt")).collect()
        self.assertEqual(result[0]["fixed_wkt"], self.valid_wkt)

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestMakeWktValid("test_invalid_wkt"))
    suite.addTest(TestMakeWktValid("test_valid_wkt"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()



# COMMAND ----------


############
## DEFINITIONS
#############


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
  if fp.exists() and (fp.stat().st_size > 1_000_000):
    # fp.unlink(missing_ok=True) # <- testing
    return geom_path_fuse
  
  # - make dirs and write the WKT
  fp.parents[0].mkdir(parents=True, exist_ok=True)
  with fp.open(mode="w", encoding="utf-8") as f:
    f.write(geom_wkt)
  return geom_path_fuse 




# COMMAND ----------

############
## UNIT TEST ###
#############

import unittest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
import pandas as pd
from pyspark.sql.functions import col, lit, udf
from pandas.testing import assert_frame_equal

class TestComplexWriteUDF(unittest.TestCase):

    @patch("pathlib.Path.open", new_callable=mock_open)
    @patch("pathlib.Path.exists", return_value=False)
    @patch("pathlib.Path.stat")
    @patch("pathlib.Path.mkdir")
    def test_write_new_wkt(self, mock_mkdir, mock_stat, mock_exists, mock_open):
        # Test writing a new WKT to a file
        geom_wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        geom_path_fuse = "/mock/path/geom_wkt.txt"

        expected = geom_path_fuse

        # Mock stat to return a small file size
        mock_stat.return_value.st_size = 1_500_000
        
        # Run the UDF
        result = complex_write_udf(geom_wkt, geom_path_fuse)
        
        # Assert that the directory creation and file writing happened
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_open.assert_called_once_with(mode="w", encoding="utf-8")
        mock_open().write.assert_called_once_with(geom_wkt)
        self.assertEqual(result, expected)

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.open", new_callable=mock_open)
    @patch("pathlib.Path.exists", return_value=True)
    @patch("pathlib.Path.stat")
    def test_skip_existing_large_file(self, mock_stat, mock_exists, mock_open, mock_mkdir):
        # Test skipping write if file exists and is large
        geom_wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        geom_path_fuse = "/mock/path/geom_wkt.txt"

        expected = geom_path_fuse

        # Mock stat to return a large file size
        mock_stat.return_value.st_size = 1_500_000
        
        # Run the UDF
        result = complex_write_udf(geom_wkt, geom_path_fuse)

        # Assert that no directory creation or file writing happened
        mock_mkdir.assert_not_called()
        mock_open.assert_not_called()
        mock_open().write.assert_not_called()
        self.assertEqual(result, expected)

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestComplexWriteUDF("test_write_new_wkt"))
    suite.addTest(TestComplexWriteUDF("test_skip_existing_large_file"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()


# COMMAND ----------



###########
## DEFINITIONS ###
############

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

############
## UNIT TEST
#############

import unittest
from unittest.mock import patch, mock_open
from shapely.geometry import Polygon
from shapely import wkt

class TestComplexCoreUDF(unittest.TestCase):
    # Mock didn't occur with builtins.open
    @patch("__main__.open", mock_open(read_data="POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))
    @patch("shapely.wkt.load")
    def test_core_true(self, mock_wkt_load):
        # Mock the WKT loading from the file
        geom_wkt = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
        mock_wkt_load.return_value = wkt.loads(geom_wkt)

        # Define the cellid WKT inside the polygon
        cellid_wkt = "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"

        # Run the UDF
        result = complex_core_udf("/mock/path/geom_wkt.txt", cellid_wkt)
        # Assert that the function returns True since cellid_wkt is within the geom_wkt
        self.assertTrue(result)

    @patch("__main__.open", mock_open(read_data="POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))
    @patch("shapely.wkt.load")
    def test_core_false(self, mock_wkt_load):
        # Mock the WKT loading from the file
        geom_wkt = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
        mock_wkt_load.return_value = wkt.loads(geom_wkt)

        # Define the cellid WKT outside the polygon
        cellid_wkt = "POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))"

        # Run the UDF
        result = complex_core_udf("/mock/path/geom_wkt.txt", cellid_wkt)

        # Assert that the function returns False since cellid_wkt is outside the geom_wkt
        self.assertFalse(result)

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestComplexCoreUDF("test_core_true"))
    suite.addTest(TestComplexCoreUDF("test_core_false"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()


# COMMAND ----------


#############
### DEFINITIONS
##################


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
    return shapely.intersection(g1,g2).wkt # No intersection => return empty polygon
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
      return None # <- AM split won't help, because not an AM
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

############
## UNIT TEST #####
##########

import unittest
from shapely import wkt
from shapely.geometry import Polygon

class TestAntimeridianSafeChip(unittest.TestCase):

    def setUp(self):
        # Define some test WKTs
        self.geom_wkt_west = "POLYGON((-170 10, -160 10, -160 20, -170 20, -170 10))"  # Western Hemisphere
        self.geom_wkt_east = "POLYGON((170 10, 180 10, 180 20, 170 20, 170 10))"  # Eastern Hemisphere
        self.cellid_wkt_west = "POLYGON((-175 15, -165 15, -165 25, -175 25, -175 15))"  # Crosses AM in Western Hemisphere
        self.cellid_wkt_east = "POLYGON((175 15, -175 15, -175 25, 175 25, 175 15))"  # Crosses AM in Eastern Hemisphere

    def test_intersection_west(self):
        result = antimeridian_safe_chip(self.geom_wkt_west, self.cellid_wkt_west, debug_level=1)
        self.assertIsNotNone(result)
        inter_geom = wkt.loads(result)
        self.assertTrue(inter_geom.intersects(wkt.loads(self.geom_wkt_west)))

    def test_intersection_east(self):
        result = antimeridian_safe_chip(self.geom_wkt_east, self.cellid_wkt_east, debug_level=1)
        self.assertIsNotNone(result)
        inter_geom = wkt.loads(result)
        self.assertTrue(inter_geom.intersects(wkt.loads(self.geom_wkt_east)))

    def test_non_AM_problem_west(self):
        # Define a WKT with self-intersection but no AM problem
        cell_id_west_bad = "POLYGON((-165 10, -160 10, -165 20, -160 20, -165 10))"
        result = antimeridian_safe_chip(self.geom_wkt_west, cell_id_west_bad, debug_level=1)
        self.assertIsNone(result)

    def test_non_AM_problem_east(self):
        # Define a WKT with self-intersection but no AM problem
        geom_in_east = "POLYGON((170 10, 179 10, 179 20, 170 20, 170 10))"
        cell_id_east_bad = "POLYGON((170 10, 165 20, 165 10, 170 20, 170 10))"
        result = antimeridian_safe_chip(geom_in_east, cell_id_east_bad, debug_level=1)
        self.assertIsNone(result)

# Function to run the tests manually and register errors
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestAntimeridianSafeChip("test_intersection_west"))
    suite.addTest(TestAntimeridianSafeChip("test_intersection_east"))
    suite.addTest(TestAntimeridianSafeChip("test_non_AM_problem_west"))
    suite.addTest(TestAntimeridianSafeChip("test_non_AM_problem_east"))
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Check if any test failed, and raise SystemExit(1) if they did
    if not result.wasSuccessful():
        raise SystemExit(1)  # Register test failure with the notebook by exiting

# Execute the test suite
run_tests()

