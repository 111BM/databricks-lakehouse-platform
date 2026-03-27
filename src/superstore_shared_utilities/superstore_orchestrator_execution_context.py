# Purpose:
#   Utility module for handling orchestration execution context
#   in Superstore Medallion Lakehouse pipelines.
#
#   Provides standardized functions to:
#     - Retrieve or generate a global master_run_id for pipeline runs
#     - Generate a unique layer_run_id for individual ETL layers
#     - Return both IDs together for logging, metrics, and tracing
#
# Key Features:
#   - Supports both master-orchestrated runs and standalone execution
#   - Ensures all child orchestrators receive a consistent master_run_id
#   - Facilitates structured logging and observability
#   - Modular design separates ID management from logging and ETL logic
#
# Imports:
#   uuid: Used for generating unique identifiers for master and layer runs.
#   superstore_logger.create_master_run_id: Generates a master pipeline UUID.
#   superstore_logger.create_run_id_for_layer: Generates a layer-specific UUID.
# ==============================================================

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