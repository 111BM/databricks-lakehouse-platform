"""
===============================================================
Module: Superstore Platform Constants

Purpose:
    This module defines key constants used throughout the Superstore Medallion Lakehouse pipeline.
    These constants are crucial for the orchestration, management, and configuration of the pipeline,
    ensuring consistency across various layers and environments.

Key Features:
1. Pipeline Layers:
    - Constants representing the three main layers of the Medallion Architecture: Bronze, Silver, and Gold.
    - These layers define the flow of data, from raw data ingestion (Bronze) to structured, analytics-ready data (Gold).

2. Default Pipeline Info:
    - Provides default names and version information for the pipeline, used as a fallback if not otherwise specified.

The environment (dev, qa, prod, integration_test) is not a constant: it comes from the
SUPERSTORE_ENV job parameter, read by superstore_platform_config.get_env().

Usage:
    The constants in this module are imported across various parts of the pipeline to:
    - Control the data flow and logic for each pipeline layer.
    - Standardize pipeline metadata, ensuring that logging, metrics, and pipeline execution contexts align with the correct layer and environment.
    
    Example usage:
        from superstore_platform_constants import BRONZE_LAYER, SILVER_LAYER
        # Use BRONZE_LAYER and SILVER_LAYER for defining table names or processing logic based on the pipeline's stage.

===============================================================
"""

# ----------------------------------
# Superstore Platform Constants
# ----------------------------------

# Pipeline Layers
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"

# Default Pipeline Info (fallback only)
DEFAULT_PIPELINE_NAME = "superstore_etl_pipeline"
DEFAULT_PIPELINE_VERSION = "1.0"

