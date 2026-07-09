"""
===============================================================
Module: Superstore Platform Schema and Catalog Configuration

Purpose:
    This module provides functions to dynamically retrieve schema names and table references based 
    on the environment and catalog configuration for the Superstore Medallion Lakehouse pipeline. 
    It centralizes the environment-specific and catalog-related configurations to simplify and standardize 
    schema access across different layers (Bronze, Silver, Gold, etc.) of the pipeline.

Key Features:
1. Dynamic Schema Generation:
    - Generates schema names based on the environment (dev, qa, prod).
    - Supports different schemas for various data layers, such as Bronze, Silver, Gold, Metrics, Quarantine, etc.

2. Environment-Driven Configurations:
    - Automatically adjusts the schema names depending on the current environment (e.g., `dev_bronze`, `prod_silver`).
    - Allows for seamless switching between environments without modifying the pipeline code.

3. Catalog Support:
    - Centralizes the catalog name, making it easy to change the catalog for all schema accesses by modifying a single variable.

4. Standardized Table Access:
    - Provides a `table()` function that combines the catalog, schema, and table name for easy referencing across all layers.

Usage:
    This module should be imported to configure and access schemas for data layers and various tables, ensuring
    consistent schema usage throughout the pipeline.

    Example usage:
        from superstore_platform_config import get_bronze_schema, table
        bronze_schema = get_bronze_schema()
        bronze_table = table(bronze_schema, "customer_data")
    
    This will return the table reference for the `customer_data` table in the Bronze schema for the current environment.

===============================================================
"""

import os

def get_env():
    """Fetches the current environment (dev, qa, prod). Defaults to 'dev' if not set."""
    return os.getenv("SUPERSTORE_ENV", "dev")

def get_catalog():
    """Returns the catalog name, defaulting to 'superstore_catalog'."""
    return os.getenv("catalog", "superstore_catalog")

def get_bronze_schema():
    """Returns the schema name for the Bronze layer based on the current environment."""
    return f"{get_env()}_bronze"

def get_silver_schema():
    """Returns the schema name for the Silver layer based on the current environment."""
    return f"{get_env()}_silver"

def get_gold_schema():
    """Returns the schema name for the Gold layer based on the current environment."""
    return f"{get_env()}_gold"

def get_metrics_schema():
    """Returns the schema name for the Metrics layer based on the current environment."""
    return f"{get_env()}_metrics"

def get_quarantine_schema():
    """Returns the schema name for the Quarantine layer based on the current environment."""
    return f"{get_env()}_quarantine"

def get_audit_schema():
    """Returns the schema name for the Audit layer based on the current environment."""
    return f"{get_env()}_audit"

def get_mart_schema():
    """Returns the schema name for the Mart layer based on the current environment."""
    return f"{get_env()}_mart"

def get_kpi_schema():
    """Returns the schema name for the KPI layer based on the current environment."""
    return f"{get_env()}_kpi"

def get_features_schema():
    """Returns the schema name for the Features layer based on the current environment."""
    return f"{get_env()}_features"


def get_semantic_layer_schema():
    """Returns the schema name for the Semantic Layer based on the current environment."""
    return f"{get_env()}_semantic_layer"

def table(schema, table_name):
    """Returns the fully-qualified table reference in the form 'catalog.schema.table_name'."""
    return f"{get_catalog()}.{schema}.{table_name}"

    