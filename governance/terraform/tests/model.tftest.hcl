# ==============================================================================
# Tests for the permission model
# ==============================================================================
#
#   terraform test
#
# The provider is MOCKED, so this runs with no workspace, no credentials and no
# state. That is deliberate: the thing worth testing is the model — who ends up
# able to read what — and that is decided entirely before any API call.
#
# Assertions run against `output.access_matrix`, the rendered result, rather than
# against the locals that produce it. Testing the output means a refactor of how
# the model is assembled cannot quietly change who has access while the tests
# still pass.
#
# WHAT THESE TESTS DO NOT COVER: the three preconditions in main.tf read locals,
# and a test cannot override a local, so they cannot be made to fire from here.
# They were verified by evaluating each condition against the real model and
# against a deliberately broken variant — true on the model, false on the
# variant. The assertions below cover the same three properties directly, so a
# violation fails either way; the preconditions exist to fail the plan for
# someone who edits the model without running the tests.
# ==============================================================================

mock_provider "databricks" {}

variables {
  catalog_name = "superstore_catalog"
  environments = ["dev", "qa", "prod"]
}

# ------------------------------------------------------------------------------
# The default model: no service principal, because the pipeline's service
# principals are granted by governance/manual_grants/, not by this model.
# ------------------------------------------------------------------------------
run "default_model_shape" {
  command = plan

  assert {
    condition     = output.governed_schema_count == 27
    error_message = "Expected 9 domains x 3 environments = 27 governed schemas."
  }

  assert {
    condition     = output.writer_configured == false
    error_message = "The writer must stay absent while pipeline_service_principal is empty; granting to a principal that does not exist fails every apply."
  }

  assert {
    condition = alltrue(flatten([
      for env in var.environments : [
        for domain in ["bronze", "silver", "quarantine", "audit", "metrics", "gold", "mart", "semantic_layer", "features"] :
        contains(keys(output.access_matrix), "${var.catalog_name}.${env}_${domain}")
      ]
    ]))
    error_message = "Every <env>_<domain> schema created by superstore_catalog_and_schemas_init must be governed. An ungoverned schema is one nobody notices is open."
  }
}

# ------------------------------------------------------------------------------
# The property the backlog item actually asked for.
# ------------------------------------------------------------------------------
run "analysts_reach_the_curated_layers_only" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for env in var.environments : [
        for domain in ["gold", "mart", "semantic_layer"] :
        contains(
          keys(output.access_matrix["${var.catalog_name}.${env}_${domain}"]),
          var.role_groups.analysts
        )
      ]
    ]))
    error_message = "Analysts must reach gold, mart and semantic_layer — the modelled layers are the whole point of granting them anything."
  }

  assert {
    condition = alltrue(flatten([
      for env in var.environments : [
        for domain in ["bronze", "silver", "quarantine", "audit", "metrics", "features"] :
        !contains(
          keys(output.access_matrix["${var.catalog_name}.${env}_${domain}"]),
          var.role_groups.analysts
        )
      ]
    ]))
    error_message = "Analysts must not reach bronze, silver, quarantine or audit (row-level PII), metrics (pipeline telemetry) or features."
  }

  assert {
    condition = alltrue(flatten([
      for env in var.environments : [
        for domain in ["bronze", "silver", "quarantine", "audit", "metrics"] :
        !contains(
          keys(output.access_matrix["${var.catalog_name}.${env}_${domain}"]),
          var.role_groups.data_scientists
        )
      ]
    ]))
    error_message = "Data scientists get features on top of the analyst surface, not the raw layers."
  }

  assert {
    condition = alltrue([
      for env in var.environments :
      contains(
        keys(output.access_matrix["${var.catalog_name}.${env}_features"]),
        var.role_groups.data_scientists
      )
    ])
    error_message = "Data scientists must reach features — that is the only thing separating them from analysts."
  }
}

# ------------------------------------------------------------------------------
# The failure mode that looks correct in review.
# ------------------------------------------------------------------------------
run "select_never_travels_without_use_schema" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for schema_name, roles in output.access_matrix : [
        for principal, privileges in roles :
        contains(privileges, "USE_SCHEMA") if contains(privileges, "SELECT")
      ]
    ]))
    error_message = "A role granted SELECT without USE_SCHEMA holds an inert grant: review passes and the query still fails."
  }
}

# ------------------------------------------------------------------------------
# dev is for hands, prod is for the pipeline.
# ------------------------------------------------------------------------------
run "humans_never_write_outside_dev" {
  command = plan

  assert {
    condition = alltrue(flatten([
      for schema_name, roles in output.access_matrix : [
        for principal, privileges in roles :
        !contains(privileges, "MODIFY")
      ] if !startswith(schema_name, "${var.catalog_name}.dev_")
    ]))
    error_message = "Nothing outside dev may grant MODIFY to a human. A prod table changes because the pipeline changed it."
  }

  assert {
    condition = alltrue([
      for domain in ["bronze", "silver", "gold", "mart", "semantic_layer", "features"] :
      contains(
        output.access_matrix["${var.catalog_name}.dev_${domain}"][var.role_groups.engineers],
        "MODIFY"
      )
    ])
    error_message = "Engineers are expected to create and drop tables by hand in dev; withholding MODIFY there just moves the work into the UI."
  }
}

# ------------------------------------------------------------------------------
# The indirection through role_groups has to actually work, or a group rename
# silently grants nothing.
# ------------------------------------------------------------------------------
run "renaming_a_group_moves_the_grants" {
  command = plan

  variables {
    role_groups = {
      analysts        = "retail_analytics_consumers"
      data_scientists = "retail_ml_engineers"
      engineers       = "superstore_engineers"
    }
  }

  assert {
    condition = contains(
      keys(output.access_matrix["${var.catalog_name}.prod_gold"]),
      "retail_analytics_consumers"
    )
    error_message = "A renamed group must receive the analyst grants — the indirection exists so a rename is one edit, not nine."
  }

  assert {
    condition = !contains(
      keys(output.access_matrix["${var.catalog_name}.prod_gold"]),
      "superstore_analysts"
    )
    error_message = "The old group name must be gone after a rename, not granted alongside the new one."
  }
}

# ------------------------------------------------------------------------------
# Backlog item 1, pre-wired: setting one variable adds the writer everywhere.
# ------------------------------------------------------------------------------
run "configuring_the_service_principal_adds_the_writer" {
  command = plan

  variables {
    pipeline_service_principal = "00000000-1111-2222-3333-444444444444"
  }

  assert {
    condition     = output.writer_configured == true
    error_message = "Setting pipeline_service_principal must bring the writer into the model."
  }

  assert {
    condition = alltrue([
      for schema_name, roles in output.access_matrix :
      contains(keys(roles), "00000000-1111-2222-3333-444444444444")
    ])
    error_message = "The writer must hold grants on every governed schema; the pipeline writes to all of them."
  }

  assert {
    condition = contains(
      output.access_matrix["${var.catalog_name}.prod_semantic_layer"]["00000000-1111-2222-3333-444444444444"],
      "CREATE_FUNCTION"
    )
    error_message = "semantic_layer needs CREATE_FUNCTION — the KPI notebooks create views and may create functions there."
  }

  assert {
    condition = alltrue(flatten([
      for schema_name, roles in output.access_matrix : [
        for principal, privileges in roles :
        !contains(privileges, "MODIFY")
        if principal != "00000000-1111-2222-3333-444444444444"
      ] if !startswith(schema_name, "${var.catalog_name}.dev_")
    ]))
    error_message = "Adding the writer must not give any human MODIFY outside dev."
  }
}

# ------------------------------------------------------------------------------
# The environment whose schemas are deliberately corrupted by a seed.
# ------------------------------------------------------------------------------
run "integration_test_cannot_be_governed" {
  command = plan

  variables {
    environments = ["dev", "qa", "prod", "integration_test"]
  }

  expect_failures = [var.environments]
}
