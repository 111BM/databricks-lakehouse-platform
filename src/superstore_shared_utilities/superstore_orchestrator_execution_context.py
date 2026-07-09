"""
===============================================================
Module: Superstore Orchestrator Execution Context

Purpose:
    This module handles the orchestration execution context in the Superstore Medallion Lakehouse pipelines.
    It provides standardized methods to retrieve or generate a global `master_run_id` for the entire pipeline run,
    as well as a unique `layer_run_id` for each individual ETL layer. Both IDs are essential for logging, metrics, 
    and traceability during pipeline execution.

Key Features:
1. Master and Layer Run ID Handling:
    - Retrieves or generates a global `master_run_id` for the entire pipeline run.
    - Generates a unique `layer_run_id` for each individual ETL layer, ensuring that each layer has its own ID.
    - Facilitates traceability of individual pipeline executions and better observability through logging.

2. Supports Standalone and Orchestrated Runs:
    - The module can handle both orchestrated pipeline runs (via a master orchestrator) and standalone execution.
    - If running standalone, it automatically generates the `master_run_id` if not provided, making it versatile for different use cases.

3. Structured Logging and Metrics:
    - The `master_run_id` and `layer_run_id` are passed together for structured logging, ensuring consistent and traceable log entries.
    - These IDs are critical for tracking execution across all layers of the pipeline, from the Bronze layer through Gold.

4. Modular Design:
    - Separates ID management logic from other ETL tasks, enabling cleaner and more maintainable code.
    - Can be reused across different pipeline scripts and orchestrators, ensuring consistent execution context handling.

5. Traceability and Observability:
    - The `master_run_id` and `layer_run_id` are included in logs and metrics, providing full traceability across pipeline runs.
    - Useful for debugging, performance tracking, and monitoring pipeline health.

Imports:
    uuid: Used for generating unique identifiers for both the `master_run_id` and `layer_run_id`.
    superstore_logger.create_master_run_id: Generates a unique `master_run_id` for the entire pipeline execution.
    superstore_logger.create_run_id_for_layer: Generates a unique `layer_run_id` for each pipeline layer.

Usage:
    The module can be imported and used in Superstore pipeline scripts to ensure proper orchestration execution context:
===============================================================
"""

import uuid
import sys
sys.path.append(
    "/Workspace/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities"
)
from superstore_logger import create_master_run_id, create_run_id_for_layer

# -----------------------------
# Master Run ID Handling
# -----------------------------
def get_master_run_id(dbutils, allow_standalone=True):
    """
    Retrieves the master_run_id from notebook widgets.  
    If not provided and allow_standalone=True, generates a new one.

    Args:
        dbutils: Databricks utilities object to access notebook widgets.
        allow_standalone (bool): If True, generates a master_run_id when not passed.
    
    Returns:
        str: master_run_id for the current pipeline run.
    
    Raises:
        RuntimeError: If master_run_id is not provided and standalone mode is disabled.
    """
    # Ensure the notebook widget exists
    dbutils.widgets.text("master_run_id", "")
    
    # Retrieve the master_run_id from the widget
    master_run_id = dbutils.widgets.get("master_run_id").strip()
    
    # Handle standalone mode: generate if missing
    if not master_run_id:
        if allow_standalone:
            master_run_id = create_master_run_id()
        else:
            raise RuntimeError(
                "master_run_id not provided; must run via master orchestrator"
            )
    
    return master_run_id


# -----------------------------
# Layer Run ID Handling
# -----------------------------
def create_layer_run_id():
    """
    Generates a unique identifier for the current ETL layer.
    
    Returns:
        str: layer_run_id
    """
    return create_run_id_for_layer()


# -----------------------------
# Execution Context Helper
# -----------------------------
def get_execution_context(dbutils, allow_standalone=True):
    """
    Returns a dictionary containing both master_run_id and layer_run_id.
    This is the canonical way for all orchestrators and ETL scripts to
    retrieve execution context for logging, metrics, and traceability.

    Args:
        dbutils: Databricks utilities object to access notebook widgets.
        allow_standalone (bool): If True, generates master_run_id if not provided.
    
    Returns:
        dict: {
            "master_run_id": str,
            "layer_run_id": str
        }
    """
    master_run_id = get_master_run_id(dbutils, allow_standalone=allow_standalone)
    layer_run_id = create_layer_run_id()
    
    return {
        "master_run_id": master_run_id,
        "layer_run_id": layer_run_id
    }