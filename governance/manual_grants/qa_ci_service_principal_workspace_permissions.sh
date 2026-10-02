#!/usr/bin/env bash
# =============================================================================
# Workspace permissions for the qa CI service principal
# =============================================================================
#
# superstore-ci-qa deploys and runs qa, including the integration suite. This
# file covers what SQL cannot grant -- workspace objects, not Unity Catalog
# securables -- plus the one-time transfer of the existing qa jobs. Unity
# Catalog grants are in qa_ci_service_principal_unity_catalog_grants.sql.
#
# Deliberately a SEPARATE service principal from superstore-ci-prod, holding
# nothing in prod: a leaked qa credential cannot reach prod data. See
# docs/NON_PROD_IDENTITY.md.
#
# Run from a terminal authenticated as a workspace admin. Every step is safe to
# re-run: mkdirs is idempotent, put-acl sets the same level again, and
# `permissions update` adds to an access list rather than replacing it.
#
# The application ID is an identifier, not a credential. The service
# principal's OAuth secret lives only in GitHub secrets.
# =============================================================================
set -euo pipefail

QA_SP="177fe539-b352-4725-9133-d11bf67d7dc6"     # superstore-ci-qa
OPERATOR="bireshmoktan@gmail.com"                # keeps CAN_MANAGE on what moves
QA_ROOT="/Workspace/superstore_qa_environment"
SECRET_SCOPE="superstore"
SQL_WAREHOUSE_ID="0c5a02c28448ef5b"
QA_PIPELINE_JOB_ID="367173180312985"             # superstore_data_platform_qa
QA_INTEGRATION_JOB_ID="842114418338454"          # superstore_integration_test_qa

# 1. Shared bundle folder, outside any person's home.
#    qa used to deploy to ~/superstore_qa_environment, which resolves to the
#    deploying identity's home -- tied to one person. The service principal
#    cannot create a folder at the workspace root or grant itself access, so
#    an admin creates it once and hands it over.
echo "Creating ${QA_ROOT} and granting the qa SP CAN_MANAGE on it..."
databricks workspace mkdirs "${QA_ROOT}"
QA_ROOT_ID=$(databricks workspace get-status "${QA_ROOT}" -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["object_id"])')
databricks permissions update directories "${QA_ROOT_ID}" --json "{
  \"access_control_list\": [
    {\"service_principal_name\": \"${QA_SP}\", \"permission_level\": \"CAN_MANAGE\"}
  ]
}" > /dev/null

# 2. Secret scope: READ. The qa pipeline's source acquisition reads
#    superstore/github_pat. (The integration suite skips acquisition, so this
#    matters only when the qa pipeline itself runs.)
echo "Granting READ on secret scope '${SECRET_SCOPE}'..."
databricks secrets put-acl "${SECRET_SCOPE}" "${QA_SP}" READ

# 3. SQL warehouse: CAN_USE, for the qa copy of the freshness alert. Granted
#    explicitly: the service principal starts with no group membership.
echo "Granting CAN_USE on SQL warehouse '${SQL_WAREHOUSE_ID}'..."
databricks permissions update warehouses "${SQL_WAREHOUSE_ID}" --json "{
  \"access_control_list\": [
    {\"service_principal_name\": \"${QA_SP}\", \"permission_level\": \"CAN_USE\"}
  ]
}" > /dev/null

# 4. Ownership of the two existing qa jobs.
#    They are kept, not recreated, so their IDs and run history survive (the
#    docs cite run IDs from them). When the service principal deploys, the
#    bundle makes the deploying identity the job owner, and only a workspace
#    admin may change an owner -- the 403 the prod switch-over hit on its first
#    deploy. So the transfer happens here, once, before that deploy. The
#    operator keeps CAN_MANAGE in the same call so access is never lost.
for JOB in "${QA_PIPELINE_JOB_ID}" "${QA_INTEGRATION_JOB_ID}"; do
  echo "Transferring ownership of job ${JOB} to the qa SP..."
  databricks permissions update jobs "${JOB}" --json "{
    \"access_control_list\": [
      {\"service_principal_name\": \"${QA_SP}\", \"permission_level\": \"IS_OWNER\"},
      {\"user_name\": \"${OPERATOR}\", \"permission_level\": \"CAN_MANAGE\"}
    ]
  }" > /dev/null
done

# 5. The qa freshness alert: CAN_MANAGE.
#    Its first deploy as the service principal updates the alert and its access
#    list, which needs CAN_MANAGE. Unlike the jobs, its ownership is not moved
#    (prod's alert stayed owned by a person too, and deployed fine). Found by
#    reading `databricks bundle plan -t qa` before the first deploy, not by a
#    failed one: only the operator and admins could manage it.
QA_ALERT_ID="1131644506829641"   # [qa] Superstore pipeline freshness
echo "Granting CAN_MANAGE on the qa alert ${QA_ALERT_ID}..."
databricks permissions update alertsv2 "${QA_ALERT_ID}" --json "{
  \"access_control_list\": [
    {\"service_principal_name\": \"${QA_SP}\", \"permission_level\": \"CAN_MANAGE\"}
  ]
}" > /dev/null

echo "Done. Verify with:"
echo "  databricks permissions get directories ${QA_ROOT_ID}"
echo "  databricks secrets list-acls ${SECRET_SCOPE}"
echo "  databricks permissions get jobs ${QA_PIPELINE_JOB_ID}"
echo "  databricks permissions get jobs ${QA_INTEGRATION_JOB_ID}"
