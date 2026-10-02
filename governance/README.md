# Governance — who can access what

Everything that decides access to the platform's data lives here, kept apart
from the pipeline on purpose: permissions change a few times a year, data moves
every week, and the identity that runs pipeline code should not also hold the
power to grant access.

There are two halves, and they are **not** equal:

| Folder | What it is | Status |
|---|---|---|
| [`terraform/`](terraform/) | The **intended** permission model: per-layer grants for analysts, data scientists and engineers, plus the pipeline's writer privileges. Declarative, checked and tested on every pull request with no credentials, planned locally by a person | **Unapplied.** It grants to account groups, which cannot exist on Databricks Free Edition (no account console, no SCIM) |
| [`manual_grants/`](manual_grants/) | The grants **actually applied** for the prod CI service principal (`superstore-ci-prod`), plus the operator's read-only access | **Applied 2026-10-01.** Safe to re-run |

`manual_grants/` is a stand-in, not a second source of truth. Its schema
privileges are a subset of `writer_privileges` in
[`terraform/model.tf`](terraform/model.tf). If the Terraform model can ever be
applied, it becomes the only source of truth and `manual_grants/` should be
deleted.

## Read next

- [docs/UNITY_CATALOG_GRANTS.md](../docs/UNITY_CATALOG_GRANTS.md) — the reasoning behind the permission model
- [docs/CI_SERVICE_PRINCIPAL.md](../docs/CI_SERVICE_PRINCIPAL.md) — how the prod service principal was set up, and what verified it
- [README → Platform constraints](../README.md#platform-constraints-databricks-free-edition) — why the model cannot be applied here
