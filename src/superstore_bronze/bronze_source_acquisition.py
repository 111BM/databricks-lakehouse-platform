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

# Run mode gate.
#
# Only incremental and backfill acquire from source. A replay re-derives Silver
# and Gold from the Bronze data already held, so contacting the source is both
# wasted work and — once the vendor has aged those files out — impossible. A
# dry run must not download either: writing files into the landing volume is a
# side effect, even though nothing reaches a table yet.
#
# Exiting cleanly rather than failing keeps the downstream tasks running; they
# depend on this task succeeding, not on it acquiring anything.
sys.path.append(f"{BUNDLE_ROOT}/src/superstore_shared_utilities")
from superstore_backfill_utils import get_backfill_config, reads_from_source

backfill_config = get_backfill_config(dbutils, allow_full_refresh=False)
run_mode = backfill_config["mode"]

if not reads_from_source(backfill_config):
    dbutils.notebook.exit(
        f"SKIPPED_ACQUISITION run_mode={run_mode} (re-deriving from existing Bronze, source not contacted)"
    )

if backfill_config["dry_run"]:
    dbutils.notebook.exit(
        f"DRY_RUN_COMPLETED run_mode={run_mode} layer=source_acquisition (no files downloaded)"
    )

# COMMAND ----------

import requests

listing_url = env_cfg["source_listing_url"]

# GitHub allows 60 API requests/hour to anonymous callers, counted per source IP —
# and Serverless egresses from shared addresses we do not control, so a stranger can
# exhaust the quota and turn this task into a 403. A token lifts it to 5,000/hour.
#
# The secret is optional on purpose: a workspace that has not set it up still runs,
# just on the anonymous limit. Nothing here is a private-repo credential; the source
# is public and the token exists only to raise the rate limit.
try:
    _github_token = dbutils.secrets.get(scope="superstore", key="github_pat")
except Exception:
    _github_token = None

session = requests.Session()
session.headers["Accept"] = "application/vnd.github+json"
if _github_token:
    session.headers["Authorization"] = f"Bearer {_github_token}"
    print("github auth=token (5,000 req/hour)")
else:
    print("github auth=anonymous (60 req/hour) — create the superstore/github_pat secret to raise it")

# NOTE: the GitHub contents API returns at most 1000 entries per directory and
# truncates silently beyond that. Fine at one file per load; if a source folder
# ever approaches 1000 files, add pagination or archive older ones into subfolders.
response = session.get(listing_url, timeout=60)
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
    resp = session.get(item["download_url"], timeout=300)
    resp.raise_for_status()
    with open(landing + item["name"], "wb") as out:
        out.write(resp.content)
    print(f"landed {item['name']} ({len(resp.content):,} bytes)")

# COMMAND ----------

dbutils.notebook.exit(f"ACQUIRED {len(new_files)} file(s) for env={env}")
