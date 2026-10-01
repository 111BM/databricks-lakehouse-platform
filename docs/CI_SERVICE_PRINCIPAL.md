# CI Service Principal — prod deployed and run by a pipeline identity, not a person

> **Status (2026-10-01): complete for prod, one path not yet exercised.** Prod is
> deployed by, runs as, and is owned by the service principal; the first run as
> it succeeded (see [Verification](#verification)). Not yet exercised: its
> **write** path into Silver and Gold — that run had no new source data, so no
> MERGE executed. The first run after a new file lands in the prod source folder
> is that test; record it here when it happens.

## What

Prod is deployed by, and runs as, a Databricks **service principal** called
`superstore-ci-prod` (application ID `328034f8-49c9-4071-a093-bc4ac9466bc2`)
instead of a person's personal access token. This closes backlog item 1 for
**prod**; dev and qa deliberately still use the token (see
[What this does not cover](#what-this-does-not-cover)).

A service principal is an identity for software: it cannot log in to the UI, it
authenticates with an OAuth client ID and secret, and it holds only the
permissions it is explicitly given.

## Why

Before this, all four GitHub workflows authenticated with one secret,
`DATABRICKS_TOKEN` — a personal access token belonging to the workspace admin.

| Problem with the personal token | What the service principal changes |
|---|---|
| It carried **all** of its owner's rights — workspace admin | It holds only what the pipeline needs |
| Prod depended on one person: token expiry, rotation or offboarding stopped prod | The identity belongs to the system |
| CI's actions and the person's clicks were indistinguishable in audit logs | Automated actions are attributable to automation |
| Prod data was written, and prod jobs owned, by a person | Prod is written and owned by the pipeline identity |
| A leaked GitHub secret was a long-lived admin credential | A leak is scoped to the pipeline's grants |

There are **two** identities in a bundle, and both had to move:

- the **deploy** identity — whoever runs `databricks bundle deploy` (set in
  [`deploy.yml`](../.github/workflows/deploy.yml));
- the **run-as** identity — whoever the jobs execute as, and so whoever every
  read and write is checked against (set with `run_as` in
  [`databricks.yml`](../databricks.yml)).

They are the same principal here on purpose: an identity may always run as
itself, so no "Service Principal: User" role is needed.

## The constraint that shaped everything: Free Edition

The workspace is **Databricks Free Edition** — identified from a metastore owned
by "System user", a single `Serverless Starter Warehouse` fixed at 2X-Small, and
only workspace-local groups. Its documented limits decided several choices:

| Free Edition limit | Consequence here |
|---|---|
| No account console, no account-level APIs, no SCIM | Account groups cannot exist, so the Terraform permission model ([UNITY_CATALOG_GRANTS.md](UNITY_CATALOG_GRANTS.md)) cannot be applied — the grants are a script instead |
| No account-level APIs | GitHub OIDC federation (no stored secret) is unavailable — an OAuth secret is used |
| Metastore owned by Databricks; the user is not a metastore admin | `CREATE CATALOG` cannot be granted, and views cannot be transferred to a service principal |

Workspace-level service principals and their OAuth secrets **do** work on Free
Edition — that was the open question, settled by creating one.

## How

The order matters: each step makes the next one possible, and doing them out of
order fails in ways that are confusing to diagnose.

### 1. Stop the pipeline needing create rights it cannot have

`superstore_catalog_and_schemas_init` ran `CREATE CATALOG IF NOT EXISTS` and
`CREATE SCHEMA IF NOT EXISTS` on **every** run, so the running identity needed
those privileges permanently for objects needed once per environment. It now
checks existence first and creates only what is missing. Behaviour is unchanged
for an identity that may create; the service principal never needs to. Verified
by the qa integration suite, which drops its schemas first (create path) and runs
the pipeline twice (exists path).

### 2. Create the service principal

In the workspace UI (Settings → Identity and access → Service principals) by a
workspace admin, with entitlements **Workspace access** and **Databricks SQL
access** only. **Not** admin access: that would make a leaked secret a full
workspace admin again, and would hide which permissions the pipeline actually
needs — the point of the exercise.

### 3. Let it manage what the old identity deployed

The service principal was added with `CAN_MANAGE` to the prod target's
`permissions` in `databricks.yml`, and deployed **while deploys still ran as the
person** — so the first deploy *as* the service principal could update the jobs,
alert and folder the previous identity had created.

### 4. Grant it the data it reads and writes

Recorded in [`scripts/governance/`](../scripts/governance/):

- [`prod_ci_service_principal_unity_catalog_grants.sql`](../scripts/governance/prod_ci_service_principal_unity_catalog_grants.sql)
  — `USE CATALOG`; `USE SCHEMA, SELECT, MODIFY, CREATE TABLE` on the nine prod
  schemas (plus `CREATE FUNCTION` on the semantic layer), matching
  `writer_privileges` in `terraform/governance/model.tf`; read/write on the prod
  landing volume; ownership of the three mart tables; and read-only `SELECT` on
  every prod schema for the human operator.
- [`prod_ci_service_principal_workspace_permissions.sh`](../scripts/governance/prod_ci_service_principal_workspace_permissions.sh)
  — `READ` on the `superstore` secret scope (the GitHub source token) and
  `CAN_USE` on the SQL warehouse (the freshness alert). These are workspace
  objects, not Unity Catalog securables, so SQL cannot grant them.

Grants are on **schemas**, never tables, so a new table needs no change; and
nothing beyond `USE CATALOG` is granted on the catalog, because dev, qa and prod
share it.

### 5. Store the credential in GitHub, never in the repo

The service principal's OAuth secret was generated in the workspace UI (365-day
lifetime) and pasted straight into GitHub repository secrets
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. Repository secrets, not
environment secrets: the latter need a paid GitHub plan on a private repository.

### 6. Switch identity

One commit: the `deploy-prod` job authenticates with the client ID and secret
instead of the token, and the prod target gains `run_as`. `DATABRICKS_TOKEN` is
removed from that job, not left alongside — with two auth methods set the CLI
refuses to choose. A step logs `Deploying as: … (superstore-ci-prod)` so a
misconfigured secret shows up as the wrong identity rather than as a permission
error three steps later.

## What went wrong, and how it was fixed

Each of these was found by running the step, not anticipated.

| Problem | Cause | Fix |
|---|---|---|
| `ALTER VIEW … OWNER TO` the service principal: `PERMISSION_DENIED` | A non-admin may transfer a **view** only to a group they belong to; on Free Edition there is no metastore admin and no account group to use | Views are not transferred. They are dropped once at switch-over and recreated — and so owned — by the service principal's first run. They are saved queries holding no data |
| First prod deploy as the service principal: `only workspace admins can change the owner of a job` (403) | When the service principal deploys, the bundle makes the deploying identity the job's owner; the jobs were owned by a person, and only admins may change an owner | A workspace admin transferred ownership of both prod jobs to the service principal once, via `databricks permissions update` — the UI hides permission editing on bundle-managed jobs |
| `deploy.yml` failed YAML parsing after the edit | A `run:` value containing `: ` is read as a new key | Caught by parsing every workflow locally before committing; would otherwise have stopped **all** deploys, not only prod |
| The operator could no longer read the mart tables: `INSUFFICIENT_PERMISSIONS` | Owning the catalog and schemas lets a person GRANT on objects inside them, but not SELECT objects someone else owns — and the marts had just moved to the service principal | `SELECT` on each prod schema for the operator (section 4 of the SQL script). Read-only on purpose: prod now changes only through the pipeline |
| `metrics_business_kpi` absent from every environment | Unrelated pre-existing defect found while listing objects: its guard checks a table that is not in the mart schema, logs an error, and exits green | Split into its own task |

## Verification

First manual prod run as the service principal — run `438050822878687`,
2026-10-01, 8.2 min:

| Check | How it was verified | Result |
|---|---|---|
| Identity | job `run_as`, and the deploy log line `Deploying as: … (superstore-ci-prod)` | the service principal |
| Every task | task states, not the job status | 18/18 SUCCESS, all attempt 0 |
| Create-only-if-missing init | ran with no create privileges | clean |
| Marts rebuilt (`CREATE OR REPLACE TABLE`) | task state + row counts | consistent with Silver (customers 492, products 528) |
| KPI views recreated | `tables list` on the semantic layer | all three present, **owned by the service principal** |
| No data lost or duplicated | Silver and Gold row counts before vs after | identical on all eight tables |
| GitHub token actually read | `system.access.audit`: `getSecret` on `superstore/github_pat` by the service principal, status 200, during the run | read — not the silent anonymous fallback |

The last row matters because a green task proves nothing there:
`bronze_source_acquisition` catches a failed secret read and continues
anonymously, announcing it only with a `print` the Jobs API never returns. With
zero files to fetch, the anonymous path would have succeeded too. The audit log
is the only place the difference is visible.

**Not verified by this run:** the write path. Silver recorded
`SKIPPED / NO_DATA` for all four entities, so no MERGE into Silver or Gold ran as
the service principal. The grants for it are in place and match the Terraform
model, but they are untested until a run carries new data.

## Rollback

Revert the switch-over commit on `main`. The next deploy authenticates with the
token again and removes `run_as`. Ownership of the two jobs stays with the
service principal, which still lets the person manage them through `CAN_MANAGE`.

## Where

| Piece | Location |
|---|---|
| Deploy identity | [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml), job `deploy-prod` |
| Run-as identity, permissions | [`databricks.yml`](../databricks.yml), target `prod` |
| Grants actually applied | [`scripts/governance/`](../scripts/governance/) |
| Grants as intended (not yet applicable) | [`terraform/governance/model.tf`](../terraform/governance/model.tf), `writer_privileges` |
| Catalog / schema creation | `src/superstore_shared_utilities/superstore_catalog_and_schemas_init.ipynb` |

## What this does not cover

- **dev and qa still use the personal token.** Their bundle roots are `~/…`, a
  user's home folder; deploying them as another identity resolves `~` elsewhere
  and creates duplicate jobs instead of updating the existing ones. Moving them
  needs a shared root path and a state migration first.
- **The integration and governance workflows** still authenticate with the token.
- **The secret is long-lived.** 365 days, rotated by hand. GitHub OIDC federation
  would remove it entirely, but needs account-level APIs Free Edition does not
  have.
- **The secret's API scope is "all APIs"**, chosen because a bundle deploy calls
  many of them and too narrow a scope fails confusingly. Narrowing it is a
  follow-up once the full set is known.
- **The grants are a script, not a model.** Re-running it is safe, but nothing
  detects drift. The Terraform model becomes the source of truth if it can ever
  be applied, and the script should then be deleted.
- **The freshness alert is owned by the person** while running as the service
  principal; it still emails the person.

## See also

- [UNITY_CATALOG_GRANTS.md](UNITY_CATALOG_GRANTS.md) — the permission model these grants are a subset of
- [Databricks: OAuth M2M for service principals](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
