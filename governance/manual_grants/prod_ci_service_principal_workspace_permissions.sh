#!/usr/bin/env bash
# =============================================================================
# Workspace permissions for the prod CI service principal (backlog item 1)
# =============================================================================
#
# The two grants SQL cannot make. Secret scopes and SQL warehouses are workspace
# objects, not Unity Catalog securables, so there is no GRANT statement for them
# -- only the CLI / REST API. Everything in Unity Catalog is in the companion
# file, prod_ci_service_principal_unity_catalog_grants.sql.
#
# Run from a terminal authenticated as a workspace admin (the DEFAULT profile).
# Safe to run more than once: put-acl sets the same level again, and
# `permissions update` ADDS to a warehouse's access list rather than replacing it.
#
# The application ID below is an identifier, not a credential. The service
# principal's OAuth secret lives only in GitHub secrets.
# =============================================================================
set -euo pipefail

SP_APPLICATION_ID="328034f8-49c9-4071-a093-bc4ac9466bc2"   # superstore-ci-prod
SECRET_SCOPE="superstore"
SQL_WAREHOUSE_ID="0c5a02c28448ef5b"                        # Serverless Starter Warehouse

# 1. Secret scope: READ.
#    bronze_source_acquisition.py reads the GitHub token from
#    dbutils.secrets.get(scope="superstore", key="github_pat"). Running as the
#    service principal, that call fails without READ on the scope.
echo "Granting READ on secret scope '${SECRET_SCOPE}'..."
databricks secrets put-acl "${SECRET_SCOPE}" "${SP_APPLICATION_ID}" READ

# 2. SQL warehouse: CAN_USE.
#    The freshness alert evaluates on this warehouse. Granted explicitly rather
#    than relied on through the `users` group, because the service principal's
#    group membership was empty when it was created.
echo "Granting CAN_USE on SQL warehouse '${SQL_WAREHOUSE_ID}'..."
databricks permissions update warehouses "${SQL_WAREHOUSE_ID}" --json "{
  \"access_control_list\": [
    {\"service_principal_name\": \"${SP_APPLICATION_ID}\", \"permission_level\": \"CAN_USE\"}
  ]
}" > /dev/null

echo "Done. Verify with:"
echo "  databricks secrets list-acls ${SECRET_SCOPE}"
echo "  databricks permissions get warehouses ${SQL_WAREHOUSE_ID}"
