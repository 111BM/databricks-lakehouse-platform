# Unity Catalog Grants — a permission model, not a pile of grants

## What

Nine domains × three environments = 27 schemas in one catalog, and until now every one
of them was readable by exactly one identity: whoever ran the job. That is stable while
the team is one person and fails in two directions the moment it is not.

The model is declared in **[governance/terraform/model.tf](../governance/terraform/model.tf)**
and applied with Terraform. Every privilege is granted on a **schema** — nothing in this
model touches a table.

| Layer (`<env>_<domain>`) | Analysts | Data scientists | Engineers | Contents |
|---|---|---|---|---|
| `bronze` | — | — | read | raw arrivals, source columns, PII |
| `silver` | — | — | read | cleaned rows, still PII |
| `quarantine` | — | — | read | rows rejected by a fatal DQ rule |
| `audit` | — | — | read | duplicates removed by dedup |
| `metrics` | — | — | read | *pipeline* telemetry, not business metrics |
| `gold` | read | read | read | conformed dimensions and facts |
| `mart` | read | read | read | curated subject-area tables |
| `semantic_layer` | read | read | read | business-KPI views |
| `features` | — | read | read | model features |

In `dev`, engineers additionally hold `MODIFY` and `CREATE_TABLE`; in `qa` and `prod`
they do not. Nothing in this model grants a human write access to production — a prod
table changes because the pipeline changed it.

The `metrics` row is the one that looks wrong and is not. `<env>_metrics` holds
`silver_layer_metrics` and `gold_layer_metrics`: row counts, durations, `run_status`.
The *business* metrics are views in `semantic_layer`. A model built by reading schema
names would get this backwards.

## Why the layer is the schema, and why that matters

`superstore_catalog_and_schemas_init` creates `<env>_<domain>`, so the medallion layer
and the securable are the same object. This is the reason a per-layer model is
expressible at all — and the reason it stays small.

Privileges on a schema are inherited by the tables inside it, **including tables that do
not exist yet**. So adding a Gold fact next month needs no governance change. Had layers
been distinguished by table prefix inside one schema, every new table would have needed
its own grant, and the model would have decayed within a quarter.

## The rule the whole model rests on

**No data privilege is ever granted on the catalog.**

`dev`, `qa` and `prod` are schemas in one catalog. A single `GRANT SELECT ON CATALOG
superstore_catalog` would hand every role every layer of every environment at once —
including `prod_quarantine`, which holds raw customer names and addresses that failed
validation. It is the one mistake here that is catastrophic rather than merely wrong,
and it is also the one that looks most like a convenient fix when a colleague is blocked
on a Friday afternoon.

Catalog grants are therefore traversal only: `USE_CATALOG` for everyone, plus `BROWSE`
for engineers. A `precondition` in
**[governance/terraform/main.tf](../governance/terraform/main.tf)** fails the plan if
anything else ever appears there.

`BROWSE` is worth a sentence: it exposes the *metadata* — names, columns, comments — of
objects you cannot query. That is a discovery aid for the people who operate the
platform and a small leak of quarantine structure for the people who consume it. So
consumers do not get it.

## The chain that has to be complete

Access is not one grant. It is three, and a missing link fails closed:

```
USE CATALOG on superstore_catalog     may I traverse into the catalog?
        ↓
USE SCHEMA  on prod_gold              may I traverse into the schema?
        ↓
SELECT      on prod_gold              may I read what is inside?
```

`SELECT` without `USE_SCHEMA` is the single most common way a permission model looks
correct in review and denies access in the warehouse: the grant is genuinely there, the
traversal is not, and the error message points at the table rather than at the schema.
Every entry in the model lists `USE_SCHEMA` explicitly rather than relying on anyone
remembering, and a second `precondition` asserts the pairing across all 27 schemas.

Note the spelling difference, which costs an afternoon the first time: SQL says
`USE SCHEMA`, the Terraform provider says `USE_SCHEMA`.

## Why views change the shape of the problem

`semantic_layer` holds views over `mart` tables — `metrics_business_kpi` and friends,
built by the KPI notebooks.

While the views and the underlying tables share an owner, an analyst holding `SELECT` on
the *view* can query it **without any grant on the tables underneath**. That is what
makes "analysts read the curated surface" implementable rather than aspirational: the
aggregate is exposed and the base table is not, with no column masking and no row
filters to maintain.

It holds because of the shared owner, not because of anything in this model. If
ownership of the marts and the views ever diverges — a different service principal
creates one of them, say — the chain breaks and analysts start getting permission
errors on views they could read yesterday. That failure is confusing enough to be worth
recognising in advance.

## Why this is Terraform, and not a task in the pipeline

The first design put a grants task inside the ETL job. It was wrong, for a reason worth
recording rather than quietly fixing.

For the pipeline to apply grants, the pipeline's identity must hold `MANAGE` on the
schemas. The pipeline runs notebooks from this repository. So **anyone who can merge
code here could grant themselves read access to `prod_quarantine`** — no admin rights
needed, just a pull request. Three smaller problems follow: a transformation change and
a who-can-see-PII change would get the same reviewer, permission activity would fill the
audit log on every scheduled run, and a governance failure would sit in the same place
as a data failure.

So the two live apart. The pipeline identity never gains `MANAGE`; only the identity
that runs this Terraform holds it, and that identity never runs pipeline code.

The narrower claim is worth stating too, because "never apply grants from a pipeline" is
too strong: dbt applies grants during every build and thousands of teams run it that
way. The problem is not grants-during-a-run. It is applying an org-wide permission model
on the ETL schedule, from the ETL identity.

## What "authoritative" means

`databricks_grants` manages the **complete** set of grants on each securable it names.
Anything present in Unity Catalog and absent from `model.tf` is removed on the next
apply.

That is the property that makes this a model rather than an access-handout script. A
configuration that can only add is one that starts lying the first time someone changes
team. Drift correction is not a feature bolted on; it is what the resource does.

It cuts both ways, and the cut surprises people exactly once: **a grant made by hand in
the UI will not survive the next apply.** The plan shows the removal before it happens,
which is the intended workflow — if you disagree with a removal, the fix is to change
the model, not to re-grant by hand.

## What state buys you

Because the resource is authoritative and Unity Catalog is declarative, an apply
converges the schemas the config still names whether or not Terraform has state.

State earns its keep for the schemas the config *used to* name. Delete a domain from
`model.tf` with no state and Terraform simply forgets those grants existed — they stay
in Unity Catalog indefinitely, and no plan will ever mention them again.

So local state is adequate while the model only grows. The first *removal* is where it
silently stops describing reality. Configure the backend before then; the block is in
`versions.tf`, commented, waiting for a bucket.

## Provisioning — what has to exist first

The model grants to **account** groups. A workspace-local group cannot hold a Unity
Catalog privilege, which is a distinction that produces a confusing error rather than an
obvious one.

None of `superstore_analysts`, `superstore_data_scientists` or `superstore_engineers`
exists in this account. Creating them needs account-admin rights in the account console
(or, properly, a SCIM sync from an identity provider), and that is deliberately outside
this repository: the data team decides what a role may read, and the identity team
decides who holds the role. When those two decisions live in one place, access stops
being revoked when people move teams.

Until the groups exist, `terraform apply` fails on an unknown principal. That is the
correct behaviour and it is why nothing here degrades to a silent skip.

## What this does not fix

**It is unverified against real principals.** No account groups exist, so no apply has
ever succeeded against this model. What *is* verified: the config validates against the
provider schema, and all three policy preconditions were evaluated against the real
model and against deliberately broken variants — each returns `true` on the model and
`false` when the violation is introduced. That covers the logic. It does not cover
Unity Catalog accepting the statements.

**The shared catalog is a discipline, not a boundary.** Environment isolation here rests
entirely on never granting data privileges at catalog level. A catalog per environment
would make it structural — `superstore_prod` simply would not contain dev data — and no
precondition would be needed because the mistake would be unavailable. That is a
migration of every table reference in the platform, not a fix, so it stays a known
limitation.

**The writer is declared but inert.** `pipeline_service_principal` is empty. Backlog
item 1 has landed — prod and qa run as their own service principals — but their grants
come from the scripts in `governance/manual_grants/`, because this model cannot be
applied on Free Edition (no account groups). Setting the variable becomes meaningful
when the model can apply, and it will then need to be one principal per environment:
the single variable predates there being two (`superstore-ci-qa`, `superstore-ci-prod`).

**Applying is manual, and CI does not enforce the review gate.**
`.github/workflows/governance.yml` runs `fmt`, `validate` and `terraform test` on pull
requests that touch the model, with no Databricks credential at all — the tests mock the
provider, and each runs a plan, so the preconditions are still enforced. Since 2026-10-02
it no longer runs a live `plan`: reading every principal's grants needs owner-or-`MANAGE`
rights, which no CI identity should hold for a model that cannot be applied here. A person
runs `terraform plan` locally, under their own login. There is deliberately no apply job:
the account groups do not exist, so an automated apply on merge would fail on an unknown
principal every time, and a permanently red workflow is one people stop reading — the
same argument as not alerting on `superseded > 0`. Grants to a missing principal fail at
*apply*, not at plan, so a local plan stays useful meanwhile.

So an apply is a person running `terraform apply` in `governance/terraform`. Even once
that is automated, a branch-protection rule is what actually forces a reviewer, and that
is repository settings rather than a file here.

## See also

- **[governance/terraform/README.md](../governance/terraform/README.md)** — how to plan
  and apply, and what to check before you do
- **[governance/terraform/model.tf](../governance/terraform/model.tf)** — the model, with
  the reasoning for each layer next to it
