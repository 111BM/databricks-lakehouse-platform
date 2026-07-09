"""
==============================================================
Pytest Configuration for Superstore Unit Tests
==============================================================

Provides fixtures for PySpark unit tests:
  1. spark: SparkSession (local or Databricks)
  2. make_dbutils: Factory for creating fake dbutils objects

Tests importing this conftest.py receive these fixtures automatically.

Usage:
  def test_example(spark):
      df = spark.createDataFrame([...])
      ...
  
  def test_with_widgets(make_dbutils):
      dbutils = make_dbutils({"widget_key": "value"})
      ...
==============================================================
"""

import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Provide a SparkSession for tests. 
    
    On Databricks: uses the existing spark session.
    Outside Databricks: creates a local session with local[2] master.
    """
    from pyspark.sql import SparkSession
    
    # Check if we're running on Databricks by looking for existing spark session
    try:
        # On Databricks, there's already a spark variable in the global namespace
        # or we can detect Spark Connect
        spark_session = SparkSession.getActiveSession()
        if spark_session is not None:
            # Already have a Spark session (Databricks serverless or cluster)
            yield spark_session
            return
    except Exception:
        pass

    # Not on Databricks - create a local Spark session for unit testing
    # Ensure Spark driver AND workers use THIS interpreter (the venv's python).
    # Otherwise Spark may launch the system python and fail with
    # PYTHON_VERSION_MISMATCH (driver 3.12 vs worker 3.13).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("superstore_unit_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def make_dbutils():
    """
    Factory fixture for creating a fake dbutils object.
    
    Returns a function that takes a dict of widget values and returns
    a mock dbutils with widgets.get() that looks up values in that dict.
    
    This allows testing code that reads Databricks job widgets without
    requiring a real Databricks workspace.
    
    Usage:
        def test_something(make_dbutils):
            dbutils = make_dbutils({"widget_key": "widget_value"})
            value = dbutils.widgets.get("widget_key", "default")
            assert value == "widget_value"
    """
    def _make_dbutils(widgets_dict):
        """
        Create a fake dbutils object with the given widget values.
        
        Args:
            widgets_dict: Dictionary mapping widget names to their values
            
        Returns:
            Fake dbutils object with widgets.get() method
        """
        class FakeWidgets:
            def __init__(self, values):
                self.values = values
            
            def get(self, key, default=""):
                """Mimic dbutils.widgets.get(key, default)"""
                return self.values.get(key, default)
        
        class FakeDbutils:
            def __init__(self, widget_values):
                self.widgets = FakeWidgets(widget_values)
        
        return FakeDbutils(widgets_dict)
    
    return _make_dbutils
