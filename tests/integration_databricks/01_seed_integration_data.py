# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test — Step 1: Seed
# MAGIC
# MAGIC Writes a small, deliberately-dirty Superstore CSV into the **isolated**
# MAGIC integration-test raw volume, and clears the Auto Loader schema/checkpoint
# MAGIC locations so the pipeline ingests this seed cleanly every run.
# MAGIC
# MAGIC Routes to `integration_test_*` schemas via `SUPERSTORE_ENV=integration_test`
# MAGIC — never touches dev/qa/prod data.

# COMMAND ----------

# Isolated integration-test paths (must match configs/.../superstore_bronze_config.yaml -> env_paths.integration_test)
RAW_PATH = "/Volumes/workspace/default/my_filestore_integration_test/raw/"
SCHEMA_LOCATION = "/Volumes/workspace/default/my_filestore_integration_test/superstore_metadata/superstore_bronze_metadata/schema/"
CHECKPOINT_LOCATION = "/Volumes/workspace/default/my_filestore_integration_test/superstore_metadata/superstore_bronze_metadata/checkpoint/"

# COMMAND ----------

# Reset Auto Loader state so the seed is (re)ingested fresh on every run.
for p in [RAW_PATH, SCHEMA_LOCATION, CHECKPOINT_LOCATION]:
    try:
        dbutils.fs.rm(p, recurse=True)
    except Exception as e:
        print(f"(ok) could not remove {p}: {e}")
    dbutils.fs.mkdirs(p)

# COMMAND ----------

# Deliberately dirty seed. Header uses the ORIGINAL Superstore column names
# (bronze ingestion renames them via column_rename_map).
#
# Scenarios encoded:
#   - clean rows                          -> Silver
#   - blank Customer ID                   -> quarantine (null business key)
#   - invalid Segment "Premium" + Region  -> Silver, flagged in repaired_columns,
#     substituted to 'Unknown' at Gold. Business key is intact, and only keys are
#     fatal under severity tiers (docs/SEVERITY_TIERS.md). This row used to be
#     quarantined outright, which is exactly the behaviour that stranded its facts.
#     AA-10480's order is separately ship-before-order, so the order is quarantined.
#   - Ship Date before Order Date         -> quarantine (business rule)
#   - duplicate Order ID/Product ID       -> audit (older loses to newer)
#   - blank Product ID (row 8)            -> products AND sales quarantine.
#     Both entities key on product_id, so one row exercises the fatal-key path
#     in two tables that would otherwise never have a quarantine table at all —
#     and a table that has never been written does not exist, which broke the
#     reconciliation check before this row existed.
#   - non-numeric Sales (row 9)           -> sales quarantine with a VALID key.
#     Deliberately different in kind: customers and products are only ever
#     quarantined for null keys, so every quarantined row there is excluded from
#     per-key arrival counts anyway. A quarantined row that HAS a key is what
#     made the first reconciliation formula over-count by 1,594 on dev orders.
#     Without this row the suite cannot reach that case.
SEED_CSV = """Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit
1,CA-2026-0001,2026-01-05,2026-01-08,Standard Class,CG-12520,Claire Gute,Consumer,United States,Henderson,Kentucky,42420,South,FUR-BO-10001798,Furniture,Bookcases,Bush Bookcase,261.96,2,0.0,41.91
2,CA-2026-0002,2026-01-06,2026-01-09,Standard Class,DV-13045,Darrin Van,Corporate,United States,Los Angeles,California,90036,West,OFF-PA-10000174,Office Supplies,Paper,Easy-staple paper,51.94,3,0.0,24.43
3,CA-2026-0003,2026-01-07,2026-01-10,Second Class,,No Id Customer,Consumer,United States,Boston,Massachusetts,02101,East,TEC-PH-10002033,Technology,Phones,Phone X,99.99,1,0.0,10.00
4,CA-2026-0004,2026-01-08,2026-01-06,Standard Class,AA-10480,Bad Categorical,Premium,United States,Miami,Florida,33101,North,OFF-ST-10000760,Office Supplies,Storage,Storage box,55.50,2,0.0,5.00
5,CA-2026-0005,2026-01-20,2026-01-10,Standard Class,BH-11710,Ship Before Order,Consumer,United States,Chicago,Illinois,60601,Central,FUR-CH-10000454,Furniture,Chairs,Office chair,120.00,1,0.0,12.00
6,CA-2026-0006,2026-01-09,2026-01-12,Standard Class,CG-12520,Claire Gute,Consumer,United States,Henderson,Kentucky,42420,South,FUR-BO-10001798,Furniture,Bookcases,Bush Bookcase,261.96,2,0.0,41.91
7,CA-2026-0006,2026-01-09,2026-01-12,First Class,CG-12520,Claire Gute,Consumer,United States,Henderson,Kentucky,42420,South,FUR-BO-10001798,Furniture,Bookcases,Bush Bookcase UPDATED,261.96,2,0.0,41.91
8,CA-2026-0009,2026-01-11,2026-01-14,Standard Class,MP-17965,Missing Product,Consumer,United States,Seattle,Washington,98101,West,,Technology,Accessories,No Id Product,75.00,1,0.0,7.50
9,CA-2026-0010,2026-01-12,2026-01-15,Standard Class,BS-11590,Bad Sales Value,Corporate,United States,Denver,Colorado,80201,West,OFF-LA-10000240,Office Supplies,Labels,Label roll,not-a-number,1,0.0,5.00
"""

# COMMAND ----------

# Write the seed file directly to the Volume.
# dbutils.fs.put() writes text content without needing a local intermediate file.
seed_file = RAW_PATH.rstrip("/") + "/superstore_integration_seed.csv"

# overwrite=True is safe here because we cleared RAW_PATH in the previous cell
dbutils.fs.put(seed_file, SEED_CSV, overwrite=True)
print(f"Seed written to: {seed_file}")
print(dbutils.fs.head(seed_file, 400))

# COMMAND ----------

dbutils.notebook.exit("SEED_OK")
