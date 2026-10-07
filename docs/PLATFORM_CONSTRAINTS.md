# Platform constraints — what Databricks Free Edition blocks, and what was done instead

> Moved out of the top-level README on 2026-10-07 so the README stays a five-minute read. Content unchanged.

The workspace is **Databricks Free Edition** — documented as for non-commercial
use, with no SLA. That was established on 2026-10-01, not assumed: a metastore
owned by "System user" rather than by anyone in the account, a single
`Serverless Starter Warehouse` fixed at 2X-Small, no classic clusters, and only
workspace-local groups — which matches Databricks' published
[Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

Several items in the [productionizing backlog](PRODUCTIONIZING_BACKLOG.md) are the shape they are because of it. Each limit, what it blocks here, and what was done instead:

| Free Edition limit | What it blocks here | What was done instead |
|---|---|---|
| No account console, no SCIM, no SSO | **Account groups**, so the Terraform permission model ([docs/UNITY_CATALOG_GRANTS.md](UNITY_CATALOG_GRANTS.md)) cannot be applied | The prod and qa service principals' grants are recorded scripts ([governance/manual_grants/](../governance/manual_grants/)), granted to individual principals |
| No account-level APIs | **GitHub OIDC federation** — CI authenticating with no stored secret | An OAuth M2M secret in GitHub secrets, 365-day lifetime, rotated by hand |
| The metastore is Databricks-owned; nobody here is a metastore admin | `CREATE CATALOG`, and transferring a **view** to a service principal | The pipeline creates only what is missing, so it needs no create rights; the service principal recreated the KPI views itself and so owns them ([docs/CI_SERVICE_PRINCIPAL.md](CI_SERVICE_PRINCIPAL.md)) |
| Serverless only, 5 concurrent tasks, one 2X-Small SQL warehouse | Real scale headroom | Performance measured as it is: ~60% of runtime at 3M rows is serverless task startup |
| Non-commercial use, no SLA | Production use, by definition | — |

One constraint is GitHub's rather than Databricks': **required reviewers** —
the mechanism behind the prod approval gate — are available on private
repositories only on GitHub Enterprise
([GitHub docs](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments)).
The available substitute — a manually triggered prod deploy
(`workflow_dispatch`) — is in place since 2026-10-02: deliberate and recorded,
but not a review.

**What a paid tier would change** is infrastructure, not design: account groups
would let the Terraform model apply as written, OIDC would remove the stored
secret, an Enterprise GitHub plan or a public repository would make the approval
gate enforceable, and the concurrency cap would go. None of it requires the
pipeline, the tests or the permission model to be redesigned; the permission model in particular was written for
account groups, and only its application is blocked.
