import os

def get_env():
    return os.getenv("SUPERSTORE_ENV", "dev")

def get_catalog():
    return os.getenv("catalog", "superstore_catalog")

def get_bronze_schema():
    return f"{get_env()}_bronze"

def get_silver_schema():
    return f"{get_env()}_silver"

def get_gold_schema():
    return f"{get_env()}_gold"

def get_metrics_schema():
    return f"{get_env()}_metrics"

def get_quarantine_schema():
    return f"{get_env()}_quarantine"

def get_audit_schema():
    return f"{get_env()}_audit"

def get_mart_schema():
    return f"{get_env()}_mart"

def get_kpi_schema():
    return f"{get_env()}_kpi"

def get_features_schema():
    return f"{get_env()}_features"

def get_dashboard_schema():
    return f"{get_env()}_dashboard"

def get_semantic_layer_schema():
    return f"{get_env()}_semantic_layer"

def table(schema, table_name):
    return f"{get_catalog()}.{schema}.{table_name}"