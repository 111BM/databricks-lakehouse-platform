# Reconciliation — accounting for every Bronze row

## What

The platform claimed one invariant above all others:

```
bronze == silver + quarantine + audit
```

It does not hold under incremental loading. It holds after a full re-derivation,
and under incremental the left side is larger — some Bronze rows are in no bucket
at all.

The invariant now has four terms, the fourth derived rather than stored:

```
bronze == silver + quarantine + audit + superseded

superseded = SUM(arrivals per key - 1) - audit_rows
```

with arrivals counted over rows that **reached deduplication** — Bronze minus
quarantine, per key.

## Why the three-term form was short

Silver holds **one row per business key**. Bronze holds **one row per arrival**.
Any key arriving more than once leaves Bronze rows that Silver does not retain,
and `classify_duplicates` only ranks rows *within the batch it is given*:

| | outcome |
|---|---|
| Key arrives twice **in one run** | loser routed to audit — accounted |
| Key arrives again **in a later run** | MERGE updates in place — **no bucket** |

The superseded version is not in Silver (overwritten), not in quarantine (it was
valid), and not in audit (which only ever receives intra-batch losers).

Both flavours leak: a *changed* row overwrites the old values, and an *unchanged*
row produces no new Silver row and is not audited either.

A replay repairs it by making the whole window one batch, which converts every
inter-batch supersession into an intra-batch duplicate. That is why the shortfall
vanishes after a replay and returns as incremental loads accumulate.

**Measured:** 7 accounted for against 9 in the integration test after the second
seed; 416 short on customers and 413 on products at ~1M rows in dev.

## Why a derived term rather than a fourth table

Nothing is lost. Bronze is immutable and retains every arrival, so a superseded
version is always recoverable.

The audit table does not preserve data — it *explains* why a row is absent from
Silver. "A later version of this key won" is an explanation that can be computed
from Bronze, and materialising it would have written **921,915** extra rows in
dev alone, duplicating what Bronze already holds.

The guarantee is unchanged in strength: every Bronze row is still accounted for
by a named rule. What changed is the honesty about which explanations need a row
of their own.

The rejected alternative was Delta Change Data Feed — capture the
`update_preimage` after each merge and append it to audit. That restores the
three-term form exactly, at the cost of CDF on every Silver table, an extra read
per merge, and an audit table that grows forever with copies of Bronze.

## How

`reconciliation_sql()` builds the query; `log_reconciliation()` runs it and logs
at ERROR when it does not balance. Both in
[`superstore_reconciliation.py`](../src/superstore_shared_utilities/superstore_reconciliation.py).

Two exclusions, both learned by getting it wrong:

**Null business keys are excluded from arrival counts.** A null key cannot be
"another arrival of" anything, and those rows are quarantined as fatal, so the
quarantine term already accounts for them.

**Quarantined rows are subtracted per key.** Arrivals must be counted over rows
that reached deduplication. Counting raw Bronze arrivals *happens* to work for
customers and products, where quarantine implies a null key — and fails badly for
orders and sales, which are quarantined for date and numeric violations with
perfectly valid keys. The first version of this query reported **1,594**
superseded orders against a true residual of **zero**, breaking an invariant that
was already holding.

**Absent tables mean zero.** Quarantine and audit tables are created on first
write, so an entity that has never produced a dirty row has none. Requiring them
failed the check for a reason unrelated to reconciliation.

## Where

| Concern | Location |
|---|---|
| SQL builder (pure) | `superstore_reconciliation.reconciliation_sql` |
| Execute + log | `superstore_reconciliation.log_reconciliation` |
| Unit tests | `tests/unit/shared/test_reconciliation.py` |
| Integration check | `tests/integration_databricks/gold/assert_scd2_change.py` |

The integration check runs in `assert_scd2_change` deliberately: that is the only
point in the suite where the state is still **incremental** and the shortfall
exists. After the replay leg it is repaired, so a check placed later would verify
the formula only where it returns zero — proving it is not wrong, not that it is
right.

## Verification

**Unit:** 16 tests. Four execute the generated SQL against real frames and
reproduce both dev states in a handful of rows — incremental (short, absorbed by
`superseded`) and re-derived (exact, `superseded = 0`). One confirms a genuinely
unexplained row still **fails** the balance, so the check cannot pass vacuously.

**Integration:** three assertions per entity in an incremental state —

1. the four-term invariant balances where three terms do not
2. `superseded` equals the *independently computed* three-term shortfall
3. on customers it equals exactly **2**

The second matters most. Defined as a residual, a fourth term balances the
equation by construction and proves nothing. Pinning it against a separately
derived shortfall is what stops it becoming a fudge factor. The third makes it
concrete: seed 2 re-sends `CG-12520` (changed) and `DV-13045` (unchanged), so the
answer must be 2 from the mechanism, not from arithmetic.

**Production data:** all four entities balance in dev at ~1M rows, with
`superseded = 0` in the re-derived state.

## What this does not do

**It does not make incremental loading lossless in the three-term sense.** The
superseded rows are still only in Bronze. Anyone wanting them queryable at Silver
needs the CDF approach above.

**`superseded > 0` is not an alert.** It is the normal consequence of incremental
loading and grows as keys are re-sent. The thing worth alerting on is
`balanced = false`, which means a Bronze row exists that no rule explains.
