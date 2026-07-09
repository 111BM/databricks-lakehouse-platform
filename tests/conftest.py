"""
==============================================================
pytest Configuration & Shared Fixtures

Purpose:
    Provides reusable SHARED fixtures for all tests.
    Layer-specific fixtures are in their respective conftest.py files:
        - tests/Unit_tests/bronze/conftest.py
        - tests/Unit_tests/silver/conftest.py
        - tests/Unit_tests/gold/conftest.py
    
Fixtures Provided:
    - spark: PySpark session (session-scoped)
    - test_catalog: Isolated test catalog with auto-cleanup
    - temp_table_manager: Helper for temporary table management
    - sample_validation_rules: Generic validation rule templates
    - sample_backfill_config: Backfill configuration template
==============================================================
"""

import pytest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DateType, DoubleType, BooleanType
)
import uuid


# =====================================================
# SPARK SESSION FIXTURE
# =====================================================

@pytest.fixture(scope="session")
def spark():
    """
    Get Spark session for testing.
    In Databricks, tries multiple methods to get the session.
    Compatible with both regular Spark and Spark Connect (serverless).
    
    Returns:
        SparkSession: Configured for testing
    """
    import sys
    
    def set_log_level_safely(spark_session):
        """Set log level, handling Spark Connect gracefully"""
        try:
            spark_session.sparkContext.setLogLevel("ERROR")
        except Exception:
            # Spark Connect doesn't support sparkContext
            # This is expected on serverless compute
            pass
    
    # Method 1: Check if spark is already in globals (notebook context)
    if 'spark' in globals():
        spark_obj = globals()['spark']
        # Check if it's actually a SparkSession, not the fixture function
        if isinstance(spark_obj, SparkSession):
            set_log_level_safely(spark_obj)
            print("✅ Using global spark from notebook")
            yield spark_obj
            return
    
    # Method 2: Try to get from __main__ module (when running in notebook)
    try:
        import __main__
        if hasattr(__main__, 'spark'):
            spark_session = __main__.spark
            if isinstance(spark_session, SparkSession):
                set_log_level_safely(spark_session)
                print("✅ Using spark from __main__")
                yield spark_session
                return
    except:
        pass
    
    # Method 3: Get active session
    try:
        spark_session = SparkSession.getActiveSession()
        if spark_session:
            set_log_level_safely(spark_session)
            print("✅ Using active Spark session")
            yield spark_session
            return
    except:
        pass
    
    # Method 4: Create new session (last resort - won't work in Databricks with Spark Connect)
    print("⚠️  Warning: Creating new Spark session (may not work in Databricks)")
    spark_session = (
        SparkSession.builder
        .appName("superstore_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    set_log_level_safely(spark_session)
    yield spark_session


# =====================================================
# TEST CATALOG FIXTURE
# =====================================================

@pytest.fixture(scope="function")
def test_catalog(spark):
    """
    Create an isolated test catalog for each test.
    Automatically cleans up after the test finishes.
    
    Usage:
        def test_something(spark, test_catalog):
            spark.sql(f"CREATE TABLE {test_catalog}.bronze.customers ...")
    
    Returns:
        str: Test catalog name (e.g., "test_catalog_20240510_123456")
    """
    # ========================================================
    # SETUP PHASE: Create Isolated Test Catalog
    # ========================================================
    
    # Generate unique catalog name with timestamp + UUID
    # Example: test_catalog_20240510_143022_7f3a9b2c
    catalog_name = f"test_catalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    # Create catalog in Unity Catalog
    # IF NOT EXISTS = idempotent (safe to retry)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    
    # Set as active catalog (all subsequent queries use this catalog)
    # This ensures test doesn't accidentally write to main.default
    spark.sql(f"USE CATALOG {catalog_name}")
    
    # Create medallion architecture schemas
    # Pattern matches production: bronze → silver → gold
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")      # Raw/ingested data
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")     # Cleaned/deduplicated
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")       # Business-level aggregates
    
    # Create data quality schemas
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS quarantine") # Failed validation rules
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS audit")      # Duplicate/audit records
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS metrics")    # Pipeline observability
    
    # ========================================================
    # TEST EXECUTION: yield control to test function
    # ========================================================
    yield catalog_name  # Test runs here with isolated catalog
    
    # ========================================================
    # CLEANUP PHASE: Remove catalog and all contents
    # ========================================================
    # CRITICAL: This block ALWAYS runs (even if test fails)
    # CASCADE = Drop all schemas, tables, views in one command
    # IF EXISTS = Idempotent cleanup (safe if catalog already gone)
    # 
    # SENIOR DE NOTE: In production, you'd audit failed cleanups
    # and have a separate job to purge abandoned test catalogs.
    # Pattern: Tag test catalogs with created_at, delete after 7 days.
    # ========================================================
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
    except Exception as e:
        # Log but don't fail the test - cleanup is best-effort
        # Test itself may have already failed, we don't want to mask that
        print(f"Warning: Failed to cleanup test catalog {catalog_name}: {e}")
        # TODO: Consider logging to monitoring system for cleanup job


# =====================================================
# HELPER FIXTURES
# =====================================================

@pytest.fixture
def temp_table_manager(spark):
    """
    Helper to manage temporary tables during tests.
    Automatically cleans up created tables.
    
    Usage:
        def test_something(spark, temp_table_manager):
            table_name = temp_table_manager.create_table(
                "test_customers", 
                sample_df
            )
            # Use table_name in test
            # Automatic cleanup after test
    """
    class TempTableManager:
        def __init__(self, spark_session):
            self.spark = spark_session
            self.tables = []
        
        def create_table(self, table_name, df, catalog="main", schema="default"):
            """Create a temp table and track it for cleanup"""
            full_name = f"{catalog}.{schema}.{table_name}"
            df.write.mode("overwrite").saveAsTable(full_name)
            self.tables.append(full_name)
            return full_name
        
        def cleanup(self):
            """Drop all created tables"""
            for table in self.tables:
                try:
                    self.spark.sql(f"DROP TABLE IF EXISTS {table}")
                except Exception as e:
                    print(f"Warning: Failed to drop {table}: {e}")
            self.tables = []
    
    manager = TempTableManager(spark)
    yield manager
    manager.cleanup()


# =====================================================
# CONFIGURATION FIXTURES
# =====================================================

@pytest.fixture
def sample_validation_rules():
    """
    Sample data quality validation rules.
    Generic template - layer-specific rules in layer conftest.py
    
    Returns:
        dict: Validation rules for testing
    """
    return {
        "customer_id": {
            "nullable": False,
            "regex": r"^C\d+$"
        },
        "customer_name": {
            "nullable": False
        },
        "segment": {
            "nullable": False,
            "allowed_values": ["Consumer", "Corporate", "Home Office"]
        },
        "country": {
            "nullable": True
        }
    }


@pytest.fixture
def sample_backfill_config():
    """
    Sample backfill configuration for testing.
    
    Returns:
        dict: Backfill config
    """
    return {
        "is_backfill": False,
        "mode": "incremental",
        "start_date": None,
        "end_date": None,
        "dry_run": False
    }


# =====================================================
# PYTEST HOOKS
# =====================================================

def pytest_configure(config):
    """
    Add custom markers to pytest.
    """
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (slower, multi-component)"
    )
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests (slowest, full pipeline)"
    )
    config.addinivalue_line(
        "markers", "slow: Slow-running tests"
    )
    config.addinivalue_line(
        "markers", "smoke: Smoke tests (run first)"
    )


def pytest_collection_modifyitems(config, items):
    """
    Auto-mark tests based on their location.
    """
    for item in items:
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
