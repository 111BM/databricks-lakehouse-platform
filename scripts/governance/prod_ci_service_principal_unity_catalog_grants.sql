-- =============================================================================
-- Unity Catalog access for the prod CI service principal (backlog item 1)
-- =============================================================================
--
-- Principal: superstore-ci-prod
--            application ID 328034f8-49c9-4071-a093-bc4ac9466bc2
--            (an identifier, not a credential: its OAuth secret lives only in
--             GitHub secrets and must never be written to this repo)
--
-- Run in the Databricks SQL editor, on the Serverless Starter Warehouse, as an
-- identity that owns these securables (today: the catalog owner). Safe to run
-- more than once: repeating a GRANT or an OWNER change is a no-op.
--
-- Companion: prod_ci_service_principal_workspace_permissions.sh covers the two
-- things SQL cannot grant: the secret scope and the SQL warehouse.
--
-- WHY THIS IS A SCRIPT AND NOT TERRAFORM. terraform/governance/model.tf already
-- declares these schema privileges as `writer_privileges`, waiting for
-- var.pipeline_service_principal. It cannot be applied: the model also grants
-- to three account groups, and account groups cannot exist on this workspace
-- (Databricks Free Edition has no account console and no SCIM). Until it can,
-- this file is the record of what the service principal holds. If the model is
-- ever applied, it becomes the source of truth and this file should go.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Table data: superstore_catalog and the nine prod schemas.
--
-- Granted on SCHEMAS, never on tables: privileges inherit to every table in
-- the schema, so a new Gold fact needs no change here. Nothing beyond
-- USE CATALOG on the catalog either: dev, qa and prod share this catalog, and
-- a data privilege at catalog level would reach every environment.
--
-- No CREATE SCHEMA and no CREATE CATALOG: superstore_catalog_and_schemas_init
-- now only creates what is missing, and in prod nothing is.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG superstore_catalog TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_bronze     TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_silver     TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_gold       TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_quarantine TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_audit      TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_metrics    TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_mart       TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA superstore_catalog.prod_features   TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;

-- The semantic layer holds the KPI views, and gets CREATE FUNCTION as in model.tf.
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION ON SCHEMA superstore_catalog.prod_semantic_layer TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;


-- -----------------------------------------------------------------------------
-- 2. Files: the prod landing volume, in the separate `workspace` catalog.
--
-- Holds the raw CSVs that bronze_source_acquisition downloads, plus Auto
-- Loader's schema location and checkpoint, so it needs both read and write.
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG workspace TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT USE SCHEMA ON SCHEMA workspace.default TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.default.my_filestore_prod TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;


-- -----------------------------------------------------------------------------
-- 3. Ownership of the objects the pipeline rebuilds.
--
-- These are written with CREATE OR REPLACE, and replacing a Unity Catalog
-- object requires owning it: MODIFY is not enough. The person running this
-- keeps full control, because they own the catalog and schemas these live in.
--
-- metrics_business_kpi is deliberately absent: it has never been created in
-- any environment (its guard checks a table that does not exist in the mart
-- schema, logs an error and exits green). Once that is fixed, the service
-- principal will create it, and so own it, with no transfer needed.
-- -----------------------------------------------------------------------------
ALTER TABLE superstore_catalog.prod_mart.mart_customer_360        OWNER TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
ALTER TABLE superstore_catalog.prod_mart.mart_product_performance OWNER TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;
ALTER TABLE superstore_catalog.prod_mart.mart_sales_daily         OWNER TO `328034f8-49c9-4071-a093-bc4ac9466bc2`;

-- The three KPI views (customer_kpi, metrics_daily_kpi, product_insights_kpi)
-- are deliberately NOT transferred here. Unity Catalog lets a non-admin give a
-- view only to a group they belong to, and on Free Edition the owner is not a
-- metastore admin and no account groups can exist -- so ALTER VIEW ... OWNER TO
-- a service principal fails with PERMISSION_DENIED. Instead the views were
-- dropped once at switch-over and recreated, and so owned, by the service
-- principal's first run. Re-running this file never needs to touch them.


-- -----------------------------------------------------------------------------
-- 4. Read access for the human operator on every prod schema.
--
-- Ownership of prod objects is moving to the service principal: the three mart
-- tables were transferred above, and anything the service principal creates
-- (the recreated KPI views, any new table) is owned by it from the start.
-- Owning the catalog and the schemas lets a person GRANT on the objects inside,
-- but does NOT give them SELECT on objects someone else owns. Without this,
-- the operator cannot read prod marts or views at all, which was discovered
-- when counting the marts failed with INSUFFICIENT_PERMISSIONS right after
-- the transfer.
--
-- SELECT only, never MODIFY. Prod data now changes only through the pipeline,
-- which is the point of moving it to a service principal. This matches the
-- Terraform model (terraform/governance/model.tf): engineers read every layer
-- and write only in dev.
--
-- Granted on SCHEMAS, like everything else here, so new tables are covered
-- automatically. Granted to the person directly only because account groups
-- cannot exist on Free Edition; in the Terraform model this is the
-- superstore_engineers group.
-- -----------------------------------------------------------------------------
GRANT SELECT ON SCHEMA superstore_catalog.prod_bronze         TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_silver         TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_gold           TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_quarantine     TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_audit          TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_metrics        TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_mart           TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_features       TO `bireshmoktan@gmail.com`;
GRANT SELECT ON SCHEMA superstore_catalog.prod_semantic_layer TO `bireshmoktan@gmail.com`;
