-- =============================================================================
-- Unity Catalog access for the qa CI service principal
-- =============================================================================
--
-- Principal: superstore-ci-qa
--            application ID 177fe539-b352-4725-9133-d11bf67d7dc6
--            (an identifier, not a credential -- its OAuth secret lives only in
--             GitHub secrets)
--
-- Run in the Databricks SQL editor, on the Serverless Starter Warehouse, as the
-- catalog owner. Safe to re-run: repeating a GRANT is a no-op.
--
-- Companion: qa_ci_service_principal_workspace_permissions.sh (folder, secret
-- scope, warehouse, job ownership -- things SQL cannot grant).
--
-- The qa service principal holds NOTHING in prod. It is a separate identity
-- from superstore-ci-prod so that a leaked qa credential cannot read or change
-- prod data. See docs/NON_PROD_IDENTITY.md.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. The catalog: USE CATALOG and CREATE SCHEMA.
--
-- CREATE SCHEMA is the one privilege prod's service principal does not hold,
-- and the reason is the integration suite: its reset task DROPS the nine
-- integration_test_* schemas at the start of every run, and the pipeline
-- recreates them. A schema can only be dropped by its owner, so the qa service
-- principal must own them -- which it does by creating them.
--
-- Accepted trade-off: CREATE SCHEMA lets it create a new schema of any name in
-- this shared catalog. It does NOT give it access to any existing schema; prod
-- schemas stay unreachable.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG superstore_catalog TO `177fe539-b352-4725-9133-d11bf67d7dc6`;


-- -----------------------------------------------------------------------------
-- 2. The nine qa schemas.
--
-- Same writer privileges as prod's service principal holds on prod, matching
-- writer_privileges in governance/terraform/model.tf. Nothing to transfer here:
-- qa holds only a handful of tables, no marts and no views, so nothing the
-- pipeline rebuilds with CREATE OR REPLACE is owned by someone else.
-- -----------------------------------------------------------------------------
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_bronze     TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_silver     TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_gold       TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_quarantine TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_audit      TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_metrics    TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_mart       TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.qa_features   TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION ON SCHEMA superstore_catalog.qa_semantic_layer TO `177fe539-b352-4725-9133-d11bf67d7dc6`;


-- -----------------------------------------------------------------------------
-- 3. The integration_test_* schemas: deliberately no grants.
--
-- They are dropped and recreated by every integration run. Before the first run
-- as the service principal, the operator drops them once; the run recreates
-- them, so the service principal owns them from then on and needs no grant.
-- Granting on schemas about to be dropped would be noise.
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- 4. Files: the qa and integration-test volumes, in the `workspace` catalog.
--
-- The integration suite's seed writes raw CSVs and its reset deletes the whole
-- volume tree, and Auto Loader keeps schema and checkpoint state there -- so
-- read and write on both.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG workspace TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT USE SCHEMA ON SCHEMA workspace.default TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.default.my_filestore_qa               TO `177fe539-b352-4725-9133-d11bf67d7dc6`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.default.my_filestore_integration_test  TO `177fe539-b352-4725-9133-d11bf67d7dc6`;


-- -----------------------------------------------------------------------------
-- 5. Read access for the human operator on the qa and integration_test schemas.
--
-- Once the service principal creates or owns objects, owning the catalog does
-- not let a person SELECT them (found on prod). Granted on schemas so objects
-- created later are covered. The integration_test_* grants are applied AFTER the
-- first service-principal run recreates those schemas -- they do not exist
-- between the operator's drop and that run.
-- -----------------------------------------------------------------------------
GRANT SELECT ON SCHEMA superstore_catalog.qa_bronze         TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_silver         TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_gold           TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_quarantine     TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_audit          TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_metrics        TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_mart           TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_features       TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.qa_semantic_layer TO `bireshmoktan@gmail.com`;
