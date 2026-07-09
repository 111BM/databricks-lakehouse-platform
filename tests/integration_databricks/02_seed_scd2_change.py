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

# COMMAND ----------

RAW_PATH = "/Volumes/workspace/default/my_filestore_integration_test/raw/"

# COMMAND ----------

# NOTE: no dbutils.fs.rm here — we keep the existing checkpoint so this is an
# incremental append, not a fresh reload.

# Row IDs continue from the first seed (8, 9). Same header/columns.
SEED_V2_CSV = """Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit
8,CA-2026-0007,2026-02-01,2026-02-04,Standard Class,CG-12520,Claire Gute,Consumer,United States,Oakland,California,94601,West,FUR-BO-10001798,Furniture,Bookcases,Bush Bookcase,261.96,2,0.0,41.91
9,CA-2026-0008,2026-02-01,2026-02-04,Standard Class,DV-13045,Darrin Van,Corporate,United States,Los Angeles,California,90036,West,OFF-PA-10000174,Office Supplies,Paper,Easy-staple paper,51.94,3,0.0,24.43
"""

# COMMAND ----------

# Write the seed file directly to the Volume using dbutils.fs.put()
seed_file = RAW_PATH.rstrip("/") + "/superstore_integration_seed_v2.csv"

dbutils.fs.put(seed_file, SEED_V2_CSV, overwrite=True)

print(f"Seed v2 written to: {seed_file}")
print(dbutils.fs.head(seed_file, 400))

# COMMAND ----------

dbutils.notebook.exit("SEED_V2_OK")
