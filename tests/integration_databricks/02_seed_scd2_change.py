# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Step 2 seed: SCD2 change + idempotency
# MAGIC
# MAGIC Adds a SECOND file to the same isolated raw volume for a second,
# MAGIC incremental pipeline run. It does NOT reset the Auto Loader
# MAGIC checkpoint/schema location — so only this new file is ingested, exactly
# MAGIC like a real incremental load.
# MAGIC
# MAGIC Two rows:
# MAGIC - `CG-12520` with a CHANGED city (Henderson -> Oakland) -> should create a
# MAGIC   new SCD2 version and close the old one.
# MAGIC - `DV-13045` UNCHANGED -> should create NO new version (idempotency).
# MAGIC
# MAGIC ## This file also carries a column the first seed did not
# MAGIC
# MAGIC `Discount Reason` is deliberate: it is the ONLY way this suite can prove
# MAGIC the schema-drift detector fires, rather than only prove it stays quiet.
# MAGIC A detector that has never reported anything is indistinguishable from one
# MAGIC that cannot — the lesson from the freshness alert, which evaluated on
# MAGIC schedule for days while structurally unable to breach its own threshold.
# MAGIC
# MAGIC It simulates the real event: a source that has been stable for months
# MAGIC starts sending a new field. Nothing declares it, so Bronze keeps it,
# MAGIC the entity split drops it, and `schema_drift` should record it as `NEW`.
# MAGIC
# MAGIC **This also tests something not previously exercised at all**: Auto Loader
# MAGIC runs with `schemaEvolutionMode = addNewColumns`, and nothing in
# MAGIC `bronze_ingest_superstore_module_01` handles `UnknownFieldException` or a
# MAGIC stream restart. If Databricks fails the stream on first sight of an
# MAGIC unknown column, this load fails — and that is a finding about the
# MAGIC pipeline, not a broken test. See docs/SCHEMA_DRIFT.md.

# COMMAND ----------

RAW_PATH = "/Volumes/workspace/default/my_filestore_integration_test/raw/"

# COMMAND ----------

# NOTE: no dbutils.fs.rm here — we keep the existing checkpoint so this is an
# incremental append, not a fresh reload.

# Row IDs continue from the first seed (10, 11).
#
# The header carries ONE extra column the first seed did not: `Discount Reason`,
# appended last. Appended rather than inserted mid-header on purpose — a new
# field arriving at the end is what an additive source change actually looks
# like, and inserting one would additionally test positional parsing, which is a
# different failure and would muddy what a red run means.
SEED_V2_CSV = """Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit,Discount Reason
10,CA-2026-0007,2026-02-01,2026-02-04,Standard Class,CG-12520,Claire Gute,Consumer,United States,Oakland,California,94601,West,FUR-BO-10001798,Furniture,Bookcases,Bush Bookcase,261.96,2,0.0,41.91,none
11,CA-2026-0008,2026-02-01,2026-02-04,Standard Class,DV-13045,Darrin Van,Corporate,United States,Los Angeles,California,90036,West,OFF-PA-10000174,Office Supplies,Paper,Easy-staple paper,51.94,3,0.0,24.43,loyalty
"""

# COMMAND ----------

# Write the seed file directly to the Volume using dbutils.fs.put()
seed_file = RAW_PATH.rstrip("/") + "/superstore_integration_seed_v2.csv"

dbutils.fs.put(seed_file, SEED_V2_CSV, overwrite=True)

print(f"Seed v2 written to: {seed_file}")
print(dbutils.fs.head(seed_file, 400))

# COMMAND ----------

dbutils.notebook.exit("SEED_V2_OK")
