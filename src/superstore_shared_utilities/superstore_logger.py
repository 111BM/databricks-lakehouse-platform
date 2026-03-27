import logging
import json
import uuid
import pytz
from datetime import datetime
import os

import sys
sys.path.append('/Users/bireshmoktan@gmail.com/superstore_medallionarchitecture_dab/src/superstore_shared_utilities')
from superstore_platform_constants import BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER, DEFAULT_PIPELINE_NAME, DEFAULT_PIPELINE_VERSION

# -----------------------------
# Helper functions to read runtime env
# -----------------------------
def get_env() -> str:
    """Return environment of the pipeline: dev, qa, prod"""
    return os.getenv("SUPERSTORE_ENV", "dev")  # fallback only if not set

def get_pipeline_name() -> str:
    """Return the pipeline name from environment variable"""
    return os.getenv("SUPERSTORE_PIPELINE_NAME", DEFAULT_PIPELINE_NAME)

def get_pipeline_version() -> str:
    """Return the pipeline version from environment variable"""
    return os.getenv("SUPERSTORE_PIPELINE_VERSION", DEFAULT_PIPELINE_VERSION)

# -----------------------------
# Generate Global Run IDs
# -----------------------------
def generate_run_id():
    """
    Generates a unique run ID for every pipeline run.
    """
    return str(uuid.uuid4())

# -----------------------------
# Custom JSON Formatter for Logs
# -----------------------------
class superstoreJsonFormatter(logging.Formatter):
    def format(self, record):
        """
        Converts the log record timestamp from UTC to Australia/Sydney timezone,
        formats the log as a structured JSON string, and includes metadata if present.
        """
        # Convert UTC timestamp from logging record to datetime
        utc_dt = datetime.utcfromtimestamp(record.created)
        # Set Sydney timezone
        sydney_tz = pytz.timezone("Australia/Sydney")
        # Convert UTC -> Sydney timezone
        sydney_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(sydney_tz)
        # ISO 8601 string with timezone offset
        timestamp_str = sydney_dt.isoformat()

        # Extract layer from metadata if present
        layer = getattr(record, "metadata", {}).get("layer")

        # Log record - Add both master_run_id and layer_run_id
        log_record = {
            "timestamp": timestamp_str,
            "env": get_env(),
            "pipeline": get_pipeline_name(),
            "pipeline_version": get_pipeline_version(),
            "master_run_id": getattr(record, "master_run_id", None),  # None if not passed
            "layer_run_id": getattr(record, "layer_run_id", None),    # None if not passed
            "layer": layer,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage()
        }

        # Add remaining metadata (without overwriting 'layer')
        if hasattr(record, "metadata"):
            for k, v in record.metadata.items():
                if k != "layer":
                    log_record[k] = v

        # Include exception info if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

# -----------------------------
# Logger Factory Function
# -----------------------------
def get_superstore_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Returns a logger configured with superstoreJsonFormatter.
    Ensures only one StreamHandler is attached per logger.
    
    Args:
        name (str): Logger name, usually the module name.
        level (logging.LEVEL): Logging level (INFO, DEBUG, WARNING, etc.)
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(superstoreJsonFormatter())
        logger.addHandler(handler)
    return logger

# -----------------------------
# Unified Logging Helper
# -----------------------------
def log_event(logger: logging.Logger, level: str, message: str, layer: str, master_run_id=None, layer_run_id=None, **kwargs):
    """
    Logs a structured JSON event with Sydney timestamp, master_run_id, layer_run_id, layer and optional metadata.
    
    Args:
        logger (logging.Logger): The logger instance.
        level (str): Log level: INFO, WARNING, ERROR, DEBUG.
        message (str): Main log message.
        master_run_id (str, optional): The global run ID for the entire pipeline.
        layer_run_id (str, optional): The run ID for the current pipeline layer.
        layer (str): The layer name for current pipeline run.
        **kwargs: Any extra metadata to include in the log (e.g., batch_id, source_path, rows).
    """
    # Include layer inside metadata if provided
    metadata = kwargs.copy()
    if layer:
        metadata["layer"] = layer

    # Add master_run_id and layer_run_id to metadata
    extra = {"metadata": metadata, "master_run_id": master_run_id, "layer_run_id": layer_run_id}
    level = level.upper()

    if level == "INFO":
        logger.info(message, extra=extra)
    elif level == "WARNING":
        logger.warning(message, extra=extra)
    elif level == "ERROR":
        logger.error(message, extra=extra)
    elif level == "DEBUG":
        logger.debug(message, extra=extra)
    else:
        logger.info(message, extra=extra)

# -----------------------------
# Helper for Layer-Orchestrator Integration
# -----------------------------
def create_run_id_for_layer():
    """
    Create a unique run ID for a specific layer. Each layer generates its own unique ID.
    """
    return generate_run_id()

# -----------------------------
# Helper for Master-Orchestrator Integration
# -----------------------------
def create_master_run_id():
    """
    Create a unique run ID for the entire pipeline (Master Orchestrator).
    """
    return generate_run_id()
