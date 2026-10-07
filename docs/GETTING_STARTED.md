# Getting started — run it as a developer, or set it up in a new workspace

> Moved out of the top-level README on 2026-10-07 so the README stays a five-minute read. Content unchanged.

**Prerequisites:** a Databricks workspace (serverless) where you are an admin, Python 3.12, and the Databricks CLI at the version CI pins (**v1.19.0** — bundle state written by one CLI version may not be readable by an older one):

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v1.19.0/install.sh | sudo sh
databricks auth login --host https://<your-workspace>.cloud.databricks.com   # OAuth, no stored token
```

### Run it as a developer

```bash
git clone https://github.com/111BM/databricks-lakehouse-platform.git
cd databricks-lakehouse-platform

databricks bundle validate --target dev
databricks bundle deploy   --target dev          # dev is deployed by you, not by CI
databricks bundle run superstore_data_platform --target dev

pytest tests/unit/ -v                                           # local, seconds
databricks bundle run superstore_integration_test --target qa   # Databricks, ~40 min
```

### First-time setup in a new workspace

The bundle resolves workspace-specific objects **by name** (`variables` in `databricks.yml`), so another workspace needs objects with these names — and two values edited:

| Create | Name | How |
|---|---|---|
| Service principal for prod | `superstore-ci-prod` | Settings → Identity and access; workspace + SQL access, **no admin** |
| Service principal for qa | `superstore-ci-qa` | same |
| Slack notification destination | `superstore-data-platform-alerts` | Settings → Notifications → Notification destinations (holds the Slack webhook URL) |
| SQL warehouse | `Serverless Starter Warehouse` | exists by default on serverless workspaces |

| Edit | Where |
|---|---|
| Workspace host | `workspace.host` in each target of `databricks.yml` |
| Operator email | `operator_email` default in `databricks.yml` |
| Freshness alert folder | `parent_path` in `resources/superstore_freshness_alert.alert.yml` → `${workspace.resource_path}`. Here it stays in the operator's home folder, where the original was created: the Alerts API cannot move an alert, so a service principal could never relocate it — but in a new workspace a service principal creates it, and cannot write into a person's home |

Then grant each service principal its access (scripts in **[governance/manual_grants/](../governance/manual_grants/)**), and add GitHub Actions repository secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` (prod), `DATABRICKS_QA_CLIENT_ID` / `DATABRICKS_QA_CLIENT_SECRET` (qa). Pushes to `qa` deploy and test; prod deploys only from **Run workflow** on `main`.

The full procedure, including the errors met on the way: **[docs/CI_SERVICE_PRINCIPAL.md](CI_SERVICE_PRINCIPAL.md)** (prod), **[docs/NON_PROD_IDENTITY.md](NON_PROD_IDENTITY.md)** (qa and dev), **[docs/DATA_QUALITY_ALERTS.md](DATA_QUALITY_ALERTS.md)** (alerts).
