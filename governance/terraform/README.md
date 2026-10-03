# Governance — Unity Catalog grants

Applies the per-layer permission model to `superstore_catalog`. Read
**[docs/UNITY_CATALOG_GRANTS.md](../../docs/UNITY_CATALOG_GRANTS.md)** first — it holds
the reasoning; this file holds the commands.

This configuration is **not** part of the Databricks Asset Bundle and does not run on
any schedule. It changes when the policy changes, which is a few times a year.

## Before the first apply

Three things must be true, and none of them are in this repository:

1. **The account groups exist** — `superstore_analysts`, `superstore_data_scientists`,
   `superstore_engineers`, as *account* groups (workspace-local groups cannot hold a
   Unity Catalog privilege). Created in the account console or synced from an identity
   provider. Until then every apply fails on an unknown principal, loudly and correctly.
2. **The schemas exist** — they are created by `superstore_catalog_and_schemas_init` on
   the first pipeline run for an environment. Terraform grants on schemas it does not
   own, so this is an ordering constraint once per environment, not per change.
3. **You are authenticated as an identity that owns the securables or is a metastore
   admin.** This is the only identity in the project that needs that power. The pipeline
   identity must never be used here.

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com   # OAuth, once
export DATABRICKS_CONFIG_PROFILE=DEFAULT                                # the profile to use
```

Credentials come from the environment, never from a variable — so nothing sensitive can
reach state, a saved plan, or a PR comment. Personal access tokens were revoked on
2026-10-02 and are no longer used anywhere in this project; a person runs `plan` and
`apply` under their own OAuth login (see [docs/NON_PROD_IDENTITY.md](../../docs/NON_PROD_IDENTITY.md)).
CI runs only the credential-free checks (`fmt`, `validate`, `terraform test`).

## Normal workflow

```bash
terraform init
```

```bash
terraform plan
```

Read the plan the way you would read a diff: every line is a permission someone will
gain or lose. Removals are as important as additions — see "authoritative" below.

```bash
terraform apply
```

## Checking the model without touching the warehouse

The full matrix, without connecting to anything:

```bash
terraform console <<< 'local.schema_grants["prod_gold"]'
```

After an apply, the same thing as an output:

```bash
terraform output access_matrix
```

## Three things that will surprise you once

**Privileges are spelled with underscores.** `USE_SCHEMA` here, `USE SCHEMA` in SQL.

**The resource is authoritative.** `databricks_grants` manages the *complete* set of
grants on each securable, so a grant added by hand in the UI is removed on the next
apply. This is the intended behaviour — it is what makes the model true rather than
aspirational — but it means the UI is not a valid way to give someone access. Change
`model.tf` instead.

**`integration_test` is deliberately ungoverned.** Its schemas are created, deliberately
corrupted by a seed, and dropped by the test suite. Standing human access there would
teach people that a data-quality violation in those schemas means something. A variable
validation rejects adding it.

## Policy checks run at plan time

Three `precondition` blocks in `main.tf` fail the plan before anything reaches the
warehouse:

| Check | Fails when |
|---|---|
| Catalog is traversal only | any privilege beyond `USE_CATALOG` / `BROWSE` appears at catalog level |
| `SELECT` implies `USE_SCHEMA` | a role is granted `SELECT` without the traversal that makes it work |
| Consumers stay out of raw layers | `analysts` or `data_scientists` appear on bronze, silver, quarantine, audit or metrics |

They are the plan-time equivalent of unit tests, and they are anchored on the catalog
resource so each is evaluated once rather than 27 times.

## State

Local state is fine while the model only grows. It stops being adequate the first time a
domain or environment is **removed** from the model — with no state, Terraform forgets
those grants exist and leaves them in Unity Catalog forever. The commented `backend`
block in `versions.tf` is waiting for a bucket.

Commit `.terraform.lock.hcl`. Do not commit `*.tfstate`.
