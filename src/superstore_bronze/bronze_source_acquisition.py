# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Source Acquisition (HTTP)
# MAGIC
# MAGIC Acquires source files from an external HTTP endpoint into the environment's
# MAGIC landing volume, so Auto Loader can ingest them like any other file drop.
# MAGIC
# MAGIC The source is a GitHub folder listing (`contents` API) that plays the role of
# MAGIC a vendor feed: files appear there over time, and this task lands the ones we
# MAGIC do not already have. Adding a file to the source needs no config change.
# MAGIC
# MAGIC **Idempotent by construction** — the set difference between the source listing
# MAGIC and the landing volume *is* the work list, so re-running lands nothing new.
# MAGIC
# MAGIC `integration_test` has no `source_listing_url`: that environment's data is
# MAGIC written by the seed notebook and must not be overwritten by a download.

# COMMAND ----------

import os
import sys
import yaml

env = dbutils.jobs.taskValues.get(
    taskKey="superstore_pipeline_master_run_id_init",
    key="SUPERSTORE_ENV",
)

# This notebook lives at <bundle_root>/src/superstore_bronze/<name>
NOTEBOOK_DIR = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
BUNDLE_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(NOTEBOOK_DIR))

config_path = f"{BUNDLE_ROOT}/configs/superstore_bronze_config/superstore_bronze_config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

env_cfg = config["env_paths"][env]
landing = env_cfg["raw_source_file_path"]

print(f"env={env}")
print(f"landing={landing}")

# COMMAND ----------

# Environments without a configured source acquire nothing. integration_test relies
# on this: its seed writes a controlled dataset that a download would clobber.
if "source_listing_url" not in env_cfg:
    dbutils.notebook.exit(f"SKIPPED_ACQUISITION env={env} (no source_listing_url configured)")

# COMMAND ----------

import requests

listing_url = env_cfg["source_listing_url"]

# NOTE: the GitHub contents API returns at most 1000 entries per directory and
# truncates silently beyond that. Fine at one file per load; if a source folder
# ever approaches 1000 files, add pagination or archive older ones into subfolders.
response = requests.get(listing_url, timeout=60)
response.raise_for_status()

# Only data files. This also skips the README placeholders that exist purely so
# the folders are visible in git (git does not track empty directories).
source_files = [
    item for item in response.json()
    if item["type"] == "file" and item["name"].endswith((".csv", ".csv.gz"))
]

if not source_files:
    dbutils.notebook.exit(f"NO_SOURCE_FILES env={env} at {listing_url}")

# COMMAND ----------

try:
    already_landed = {f.name for f in dbutils.fs.ls(landing)}
except Exception:
    # Landing folder does not exist yet (first run in a fresh environment)
    already_landed = set()

# Sorted so dated filenames land in chronological order
new_files = sorted(
    (f for f in source_files if f["name"] not in already_landed),
    key=lambda f: f["name"],
)

print(f"source={len(source_files)} landed={len(already_landed)} new={len(new_files)}")

# COMMAND ----------

for item in new_files:
    resp = requests.get(item["download_url"], timeout=300)
    resp.raise_for_status()
    with open(landing + item["name"], "wb") as out:
        out.write(resp.content)
    print(f"landed {item['name']} ({len(resp.content):,} bytes)")

# COMMAND ----------

dbutils.notebook.exit(f"ACQUIRED {len(new_files)} file(s) for env={env}")
