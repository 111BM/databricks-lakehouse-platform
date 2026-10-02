# Non-Prod Identity — qa on its own service principal, dev under the developer's own login

> **Status (2026-10-02): qa and dev done and verified.** qa deploys and runs as
> its own service principal; dev is deployed by the developer under OAuth, and CI
> no longer deploys it, and the governance workflow runs with no credentials.
> **No workflow uses a personal access token, and every one has been revoked**
> (2026-10-02) — see [Revocation](#revocation).

## What

Prod was already deployed and run by a service principal
([CI_SERVICE_PRINCIPAL.md](CI_SERVICE_PRINCIPAL.md)). This document covers the two
other environments:

| Environment | Identity now | Deployed by |
|---|---|---|
| **qa** | **`superstore-ci-qa`** (application ID `177fe539-b352-4725-9133-d11bf67d7dc6`), a second service principal that holds **nothing in prod** | CI, on push to `qa` |
| **dev** | **the developer**, signed in with Databricks **OAuth** (no stored token) | the developer, from their own machine: `databricks bundle deploy -t dev` |
| prod (unchanged) | `superstore-ci-prod` | CI, started by hand |

## Why

Moving prod to a service principal fixed **prod's** identity, but a **personal
access token** was still stored in GitHub, used by `deploy-dev`, `deploy-qa`, the
integration-test workflow and the governance workflow. That token carried its
owner's full rights — **workspace admin** — so a leak would have exposed prod as
well, whatever environment it was nominally "for".

The goal is that no person's long-lived credential exists anywhere: people sign in
as themselves, and automation uses non-human identities scoped to one environment.

## Which decisions were made, and why

### 1. A separate qa service principal, not prod's

| Option | Verdict |
|---|---|
| **New `superstore-ci-qa`** | **Chosen.** A leaked qa secret cannot reach prod data: the qa SP has no grant on any `prod_*` schema or the prod volume |
| Reuse `superstore-ci-prod` for qa | Rejected: qa runs far more often and from every push, so its credential is exercised more — and would then carry prod's write access |

### 2. Keep the existing qa jobs (bind), don't recreate them

| Option | Verdict |
|---|---|
| **`bundle deployment bind`** | **Chosen.** Links the existing jobs and alert into the new bundle state, so the next deploy **updates** them. Same IDs, full run history kept — the docs cite run IDs from the integration job |
| Let the bundle create new jobs, delete the old ones | Rejected: deleting a job deletes its run history, and every cited run ID would stop resolving |

### 3. A dedicated shared folder, not the service principal's home

| Option | Verdict |
|---|---|
| **`/Workspace/superstore_qa_environment/…`** | **Chosen.** Mirrors prod's layout exactly; one obvious place per environment; access granted explicitly to two identities |
| `/Workspace/Users/<sp-id>/…` (Databricks' default template) | Legitimate and slightly simpler — every identity can write its own home — but qa and prod would no longer look alike |

### 4. qa in `mode: production`

Development mode prefixes every job with the deploying identity's name
(`[dev bireshmoktan] …`) and is meant for one person's copy. Its one useful side
effect — pausing schedules — is now done **explicitly, in the qa target only**.
Setting `pause_status` in the shared job YAML instead would apply to every target;
that mistake once scheduled dev (README → CI/CD, *Schedule*).

### 5. dev deployed by the developer under OAuth — not by CI

| Option | Verdict |
|---|---|
| **Developer deploys dev, OAuth login** | **Chosen.** The pattern Databricks' bundle templates are built around, and the norm in security-mature teams: dev is personal, so the developer deploys it under their own identity, and CI holds only non-human, per-environment identities. Cost: one command instead of auto-deploy on push |
| Keep the personal token for dev auto-deploy | Rejected: the token is workspace admin, so "a token only for dev" still exposes prod; and it would be the one thing keeping a personal credential in CI |
| A third service principal for dev | Rejected: another identity, secret and grant set to maintain for an environment meant to be personal; dev would also need the same folder move and bind as qa |

### 6. Empty the integration-test schemas each run, instead of dropping them

The integration suite resets its nine `integration_test_*` schemas at the start of
every run. It used to **drop** them, which also destroyed every grant on them — so
once the qa service principal owned them, a person's read access vanished after
each test.

| Option | Verdict |
|---|---|
| **Drop the tables and views, keep the schemas** | **Chosen.** Schemas are long-lived containers that hold grants; tables are what tests create and destroy. The README backlog had already named this fix |
| A separate catalog for test environments, with catalog-level read access | Unity Catalog's recommended layout, and the better long-term shape — but it means migrating every table reference in the platform |
| The pipeline grants access on schemas it creates | Rejected: the identity that runs code should not also control access — the principle the governance design rests on |

### 7. Governance: credential-free checks in CI, `plan` run locally

| Option | Verdict |
|---|---|
| **CI keeps `fmt`, `validate`, `terraform test`; a person runs `plan` locally** | **Chosen.** No Databricks credential in the workflow at all, and nothing lost on policy: the tests mock the provider, each runs a plan, and a plan evaluates the three preconditions that block over-sharing |
| A read-only governance service principal running `plan` in CI | Rejected for now. Reading **every** principal's grants needs owner-or-`MANAGE`-level rights — effectively the power to grant access — and anything less sees only its own grants, giving a misleading diff. That is real power in CI to plan a model that cannot be applied on Free Edition |

The mature target, deferred rather than dismissed: a dedicated governance identity
(OIDC, no stored secret) that **plans on every pull request and applies on merge** —
"GitOps for permissions". It becomes worth it once account groups exist and apply is
possible.

## How it was done

The order mattered; each step makes the next possible.

| # | Who | Step |
|---|---|---|
| 1 | operator | Upgraded the local CLI to **v1.19.0**, CI's pinned version — `bind` writes bundle state, which must be in the format CI reads. The CLI had been installed by Databricks' install script, not Homebrew, so `brew upgrade` did nothing |
| 2 | operator | Created `superstore-ci-qa`: workspace and SQL access, **no admin** |
| 3 | operator | Ran [`qa_ci_service_principal_unity_catalog_grants.sql`](../governance/manual_grants/qa_ci_service_principal_unity_catalog_grants.sql): `USE CATALOG` + **`CREATE SCHEMA`** on the catalog, writer privileges on the nine `qa_*` schemas, both qa volumes |
| 4 | operator | Ran [`qa_ci_service_principal_workspace_permissions.sh`](../governance/manual_grants/qa_ci_service_principal_workspace_permissions.sh): created the shared folder and handed it over, secret scope `READ`, warehouse `CAN_USE`, ownership of the two qa jobs, `CAN_MANAGE` on the qa alert |
| 5 | operator | Dropped the nine `integration_test_*` schemas once, so the service principal would recreate — and own — them |
| 6 | operator | Generated the qa service principal's OAuth secret into GitHub as `DATABRICKS_QA_CLIENT_ID` / `DATABRICKS_QA_CLIENT_SECRET` |
| 7 | config | qa target → `mode: production`, shared `root_path`, `run_as` the qa SP, permissions, schedule paused; `deploy-qa` and the integration workflow → the qa secrets |
| 8 | CLI | `bundle deployment bind` for the two jobs and the alert; then **`bundle plan -t qa`: 3 updates, nothing created or deleted** |
| 9 | CI | First qa deploy and integration run as the service principal |
| 10 | code | Reset changed to empty the schemas rather than drop them |
| 11 | operator | `databricks auth login --profile oauth`; tokens held in the macOS keychain, none in `~/.databrickscfg` |
| 12 | CI | `deploy-dev` removed; a push to `dev` runs the unit tests only |
| 13 | CI | `governance.yml`: live `plan` and its credentials removed; `fmt`, `validate` and `terraform test` run with **no Databricks credential** — reproduced locally with every `DATABRICKS_*` variable unset, 7/7 tests passing |

**Why `CREATE SCHEMA` — the one privilege prod's service principal lacks:** a schema
can only be dropped or recreated by its owner, and the integration suite's schemas
are created by the pipeline itself. The accepted trade-off is that the qa service
principal can create a new schema of any name in the shared catalog; it gains no
access to any existing one, so prod schemas stay unreachable.

## When each piece applies

| Event | What happens |
|---|---|
| Push to `dev` | unit tests only |
| The developer wants changes in Databricks dev | `databricks bundle deploy -t dev --profile oauth` from their machine |
| Push to `qa` | unit tests → qa deployed **as `superstore-ci-qa`** → integration suite started and run **as `superstore-ci-qa`** |
| Push to `main` | unit tests only |
| Prod deploy | started by hand, deployed as `superstore-ci-prod` |

## Where

| Piece | Location |
|---|---|
| qa identity, folder, mode, schedule | [`databricks.yml`](../databricks.yml), target `qa` |
| qa deploy and integration auth | [`deploy.yml`](../.github/workflows/deploy.yml) `deploy-qa`; [`integration-tests.yml`](../.github/workflows/integration-tests.yml) |
| Why dev is not in CI | `deploy.yml`, comment where `deploy-dev` used to be |
| qa grants actually applied | [`governance/manual_grants/`](../governance/manual_grants/) |
| Integration reset | [`tests/integration_databricks/04_cleanup_integration.py`](../tests/integration_databricks/04_cleanup_integration.py) |

## What went wrong, and how it was caught

| Problem | Caught by | Fix |
|---|---|---|
| `brew upgrade databricks` did nothing | `databricks --version` still 0.295.0 afterwards | The CLI came from Databricks' install script; reinstalled from that script **pinned to `v1.19.0`**, after reading it to confirm it installs exactly that version |
| The qa service principal could not manage the qa alert | **Reading `bundle plan -t qa` before the first deploy** — not a failed deploy | `CAN_MANAGE` on the alert (step 5 of the workspace script) |
| Dropping the integration schemas destroyed a person's read grants every run | Reasoning through the reset before granting | Empty, don't drop (decision 6) |
| **The reset hid its own failures**: `except: print("(ok) could not drop …")`, in output the Jobs API never returns | Reading the reset while changing it | It now **raises**, and proves each schema is empty afterwards |
| Ownership transfer could have switched who the qa jobs *run as* before the grants existed | Reading the job settings after the transfer | Grants ran first; and development mode had pinned run-as to the person, so the switch only happened with the config change |

## Before and after

| | Before | After |
|---|---|---|
| qa deployed by | personal admin token | `superstore-ci-qa` |
| qa jobs run as / owned by | the person | `superstore-ci-qa` |
| qa job names | `[dev bireshmoktan] superstore_data_platform_qa` | `superstore_data_platform_qa` |
| qa bundle folder | the person's home folder | `/Workspace/superstore_qa_environment` |
| qa job IDs and history | — | **unchanged**: same IDs, 86 integration runs back to 2026-07-27 |
| Integration schemas | dropped and recreated each run, grants lost | emptied each run, **grants kept** |
| Reset on failure | printed "(ok)", carried on | raises |
| dev deployed by | CI with the personal token, on every push | the developer, OAuth, on demand |
| Developer's own CLI login | personal token in `~/.databrickscfg` | OAuth, tokens in the OS keychain; no `token =` line left |
| Personal token in CI | 4 jobs | **none** |
| Personal tokens in the workspace | at least one, admin, never expiring | **none — all revoked** |

## Revocation

Moving every workflow off the personal token only stopped *depending* on it. The
risk ended when the token stopped working. On 2026-10-02:

| Step | Who | Verified by |
|---|---|---|
| Deleted `DATABRICKS_TOKEN` from GitHub's repository secrets | operator | not visible without `gh`; no workflow references it, so a leftover copy could not be used by CI either |
| Revoked every personal access token in the workspace | operator | `databricks tokens list` → **0 tokens** |
| The old token is dead, not just unused | — | calling the API with the old `DEFAULT` profile → **`Invalid access token`** |
| Removed the token profile from the developer's machine | operator | `~/.databrickscfg` holds **no `token =` line**; the OAuth profile was renamed `DEFAULT` and signed in again (OAuth logins are cached under the profile name), so plain `databricks` commands work without `--profile` |
| CI still works with no personal token anywhere — **qa** | CI | qa deployed at 20:57:47, after revocation, and integration run `900516040616338` passed, started by `superstore-ci-qa` |
| CI still works with no personal token anywhere — **prod** | CI | manual prod deploy #61 completed 2026-10-03 06:53:44, after revocation; the prod job still runs as `superstore-ci-prod` |
| …and **dev** | developer | deployed from the developer's machine over OAuth |

## Verification

| Claim | Evidence |
|---|---|
| Bind kept the jobs | `bundle summary -t qa` → `367173180312985`, `842114418338454`, alert `1131644506829641`; `bundle plan` → 3 updates, 0 creates, 0 deletes |
| qa deployed as the SP | qa bundle state written 15:14:10 in the shared folder; jobs renamed, `run_as` = the qa SP, schedule PAUSED |
| History kept | 86 runs attached to `842114418338454`, oldest 2026-07-27, including cited run `801626137950969` |
| Integration suite runs as the SP | run `645322115490319`: started by the qa SP, 21/21 tasks SUCCESS, none retried; all nine `integration_test_*` schemas recreated and owned by it |
| Reset empties, keeps grants | run `252852975399843` (green): exit `CLEANUP_DONE objects_dropped=37`; all nine schema IDs unchanged; the operator's `SELECT` survived on all nine |
| dev deploys over OAuth | `bundle plan -t dev --profile oauth` → 0 changes; `bundle deploy` → 131 files uploaded, dev bundle state written 17:06:38 |

## What this does not cover

- **The governance workflow has no live `plan` in CI** (decision 7). The
  credential-free checks catch policy violations; what is lost is the diff against
  the real workspace on a pull request, which a person now produces locally.
- **The dev copy of the integration suite** shares the `integration_test_*` schemas,
  now owned by the qa service principal. Run as the developer, its reset would fail
  — loudly now — on tables it does not own. Accepted: the suite is run through qa.
- **`CREATE SCHEMA` on the shared catalog** for the qa service principal, as
  explained above. A catalog per environment would remove the trade-off.
- **The secret is long-lived** (365 days) — OIDC federation would remove it, but
  needs the account-level APIs Free Edition does not have.

## See also

- [CI_SERVICE_PRINCIPAL.md](CI_SERVICE_PRINCIPAL.md) — the same move for prod
- [CI_CONCURRENCY.md](CI_CONCURRENCY.md) — why qa's deploy and test share a concurrency group
- [UNITY_CATALOG_GRANTS.md](UNITY_CATALOG_GRANTS.md) — the permission model these grants are a subset of
- README → *Platform constraints: Databricks Free Edition*
