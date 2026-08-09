# Gold Window Alignment — one window parameter, two meanings

## What

`start_date` and `end_date` are a single pair of job parameters, but the two Gold
frameworks interpreted them against different clocks:

| framework | derived the window from | agrees with Silver? |
|---|---|---|
| facts | `bronze_ingestion_ts` | yes |
| **dimensions** | `silver_ingestion_ts` | **no** |

Silver windows on `bronze_ingestion_ts` — when the data was ingested. The dimension
framework windowed on `silver_ingestion_ts` — when the row was *written to Silver*.
One operator-supplied window therefore selected two different populations inside the
same layer.

Dimensions now use `bronze_ingestion_ts`, matching Silver and facts.

## Why it mattered

**Replay was structurally impossible for dimensions.** A replay always rewrites Silver
with a timestamp of *now*, so the rewritten rows could never fall inside a historical
window. Replaying the window the data belongs to updated Silver and selected **zero**
rows at Gold — while every task reported success.

Measured on `dev`, where every Silver product row carries `bronze_ingestion_ts` of
2026-08-08 and `silver_ingestion_ts` of 2026-08-09:

| window `2026-08-08` filtered on | rows selected |
|---|---|
| `silver_ingestion_ts` (before) | **0** of 51,511 |
| `bronze_ingestion_ts` (after) | **51,511** of 51,511 |

This was found while recovering the orphaned facts in
**[VALUE_STANDARDIZATION.md](VALUE_STANDARDIZATION.md)**: the Silver fix worked, 49,649
products were recovered, and none of them reached `dim_products`. The only way to get
them there was to replay with the *current* date, which exploits the mismatch rather
than fixing it.

It also meant a windowed run could re-derive facts for one population and dimensions for
another — a consistency hazard on top of the silent no-op.

## The rejected rationale

The original code was explicit about its choice:

> `# Derive ingestion_date from silver_ingestion_ts (NOT bronze!)`
> `# Dimensions track changes at Silver layer due to SCD2 processing`

That is a real argument — an SCD2 version *is* created when Silver detects a change — but
it conflates two separate questions:

- **When is a version dated?** Processing time, deliberately, for the reasons in
  **[SCD2_VALIDITY_DATING.md](SCD2_VALIDITY_DATING.md)**.
- **Which rows does a run re-derive?** The window the operator asked for.

The first is about the *content* of a dimension row. The second is about *selection*.
Using the Silver clock for selection made the run mode inoperable without making the
dating any more correct.

Both clocks still exist and both are still right where they belong:

| use | column | unchanged |
|---|---|---|
| incremental watermark at Gold | `silver_ingestion_ts` | yes |
| SCD2 `effective_from` | `silver_ingestion_ts` | yes |
| **window selection** | `bronze_ingestion_ts` | **changed** |

## How

`apply_window_filter(df, start_date, end_date, window_ts_col)` — a pure function
extracted from `get_incremental_silver_for_dims`. Silver tables carry no
`ingestion_date`, so it derives one and filters inclusively at both ends.

Extraction was the point as much as the fix: while the selection was welded inside an
I/O path, no unit test could reach it, which is how a defect that selected *nothing* for
two run modes went unnoticed. It now has nine tests, including the exact dev data shape
(Bronze 08-08, Silver 08-09) that exposed it.

### A second bug found by the tests

Gold compared `ingestion_date` against **datetime objects**; Silver compares against
**"YYYY-MM-DD" strings**. A naive datetime literal is converted from the *driver's*
timezone, while the derived date is rendered in the *session* timezone. Where those
differ, Gold's window shifted by a day relative to Silver's — reintroducing the same
misalignment by another route.

Surfaced because the test session pins UTC (`conftest.py`) while the machine ran AEST.
Gold now formats bounds to date strings, as Silver already did. A date string has no
timezone to convert.

The test data was also changed to build timestamps as strings cast inside Spark rather
than Python datetimes, so the suite behaves identically on a developer machine in any
timezone and in CI.

## Where

| Concern | Location |
|---|---|
| Window column constant | `superstore_gold_dimension_framework.WINDOW_TS_COL` |
| Selection | `superstore_gold_dimension_framework.apply_window_filter` |
| Call site | `get_incremental_silver_for_dims`, windowed branch |
| Tests | `tests/unit/gold/test_gold_window_filter.py` |

## What this does not address

**The facts framework duplicates this logic** with its own derive-and-filter block. It
was already on the correct clock, so it is not broken, but the two copies can drift
again. Consolidating them onto `apply_window_filter` is the obvious follow-up and was
left out to keep this change to the defect.

**No integration coverage for windowed runs.** The integration test exercises
`incremental` only, so neither this defect nor the two config bugs in
`VALUE_STANDARDIZATION.md` could have been caught by it. A replay leg would close that
class of gap permanently — it is the single highest-value addition to the test suite
right now.
