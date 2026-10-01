# ==============================================================================
# Applying the model
# ==============================================================================
#
# `databricks_grants` is AUTHORITATIVE for the securable it names: it manages the
# complete set of grants on that object, so anything present in Unity Catalog and
# absent from model.tf is removed on the next apply.
#
# That property is the reason this is a permission model rather than an
# access-handout script. A configuration that can only add is one that starts
# lying the first time someone leaves a team. It cuts both ways, and the cut is
# worth stating plainly: a grant made by hand in the UI will not survive the next
# apply. That is the intended behaviour, and it surprises people exactly once.
# ==============================================================================

# ------------------------------------------------------------------------------
# Catalog: traversal only.
#
# The policy assertions below are anchored here rather than on the per-schema
# resource because this resource has a single instance, so each one is evaluated
# once instead of twenty-seven times. They are the plan-time equivalent of unit
# tests: they fail the plan, before anything reaches the warehouse.
# ------------------------------------------------------------------------------
resource "databricks_grants" "catalog" {
  catalog = var.catalog_name

  dynamic "grant" {
    for_each = local.catalog_privileges
    content {
      principal  = local.principals[grant.key]
      privileges = grant.value
    }
  }

  lifecycle {
    # 1. No data privilege at catalog level, ever.
    #
    # One `GRANT SELECT ON CATALOG` would expose all nine layers of all three
    # environments in a single line — the one mistake in this model that is
    # catastrophic rather than merely wrong, and the one that looks most like a
    # convenient fix when someone is blocked on a Friday.
    precondition {
      condition = length(setsubtract(
        toset(flatten(values(local.catalog_privileges))),
        toset(["USE_CATALOG", "BROWSE"])
      )) == 0
      error_message = "Catalog grants must be traversal only (USE_CATALOG, BROWSE). dev, qa and prod share this catalog, so a data privilege here exposes every layer of every environment at once."
    }

    # 2. SELECT always travels with USE_SCHEMA.
    #
    # Without the pairing the grant exists, review passes, and the analyst still
    # gets permission denied — the most common way a correct-looking model fails
    # in practice.
    precondition {
      # flatten() recurses, so flattening a list of privilege-lists yields bare
      # strings and `contains` then fails at plan time rather than at validate
      # time. The comprehension is nested so that the inner level produces
      # booleans, and only those get flattened.
      condition = alltrue(flatten([
        for roles in values(local.schema_grants) : [
          for privileges in values(roles) :
          contains(privileges, "USE_SCHEMA") if contains(privileges, "SELECT")
        ]
      ]))
      error_message = "Every role granted SELECT on a schema must also hold USE_SCHEMA, or the grant is inert."
    }

    # 3. Consumer roles never reach raw or operational layers.
    #
    # bronze, silver, quarantine and audit hold row-level PII; metrics holds
    # pipeline telemetry that reads like business metrics and is not. Asserted
    # rather than merely arranged, so that a hurried edit fails the plan.
    precondition {
      condition = alltrue(flatten([
        for domain in local.consumer_restricted_domains : [
          for role in local.consumer_roles :
          !contains(keys(local.model[domain]), role)
        ]
      ]))
      error_message = "Consumer roles (analysts, data_scientists) must not be granted on bronze, silver, quarantine, audit or metrics — those hold raw PII or pipeline telemetry, not business data."
    }
  }
}

# ------------------------------------------------------------------------------
# Schemas: the per-layer model, one resource instance per <env>_<domain>.
#
# Terraform grants on schemas that already exist and does not need to own them,
# so this coexists with superstore_catalog_and_schemas_init creating them at run
# time. A new environment therefore needs one pipeline run before its first
# apply — the ordering is real but it is once per environment, not per change.
# ------------------------------------------------------------------------------
resource "databricks_grants" "schema" {
  for_each = local.schema_grants

  schema = "${var.catalog_name}.${each.key}"

  dynamic "grant" {
    for_each = each.value
    content {
      principal  = local.principals[grant.key]
      privileges = grant.value
    }
  }
}

# ------------------------------------------------------------------------------
# The model as a readable matrix.
#
# Printed by `terraform output access_matrix`, so "who can read prod_quarantine?"
# is answerable without reading HCL or querying the warehouse.
# ------------------------------------------------------------------------------
output "access_matrix" {
  description = "Effective grants per schema, by account group."
  value = {
    for schema_name, roles in local.schema_grants :
    "${var.catalog_name}.${schema_name}" => {
      for role, privileges in roles :
      local.principals[role] => privileges
    }
  }
}

output "governed_schema_count" {
  description = "Number of schemas this configuration is authoritative for."
  value       = length(local.schema_grants)
}

output "writer_configured" {
  description = "False while the pipeline still runs as a schema owner (backlog item 1)."
  value       = var.pipeline_service_principal != ""
}
