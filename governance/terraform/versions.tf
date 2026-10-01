# ==============================================================================
# Provider and state configuration
# ==============================================================================
#
# This configuration is SEPARATE from the Databricks Asset Bundle. The bundle
# also uses Terraform internally (its state lives under .databricks/), but that
# state describes jobs and workspace files. This one describes who may read
# which layer, and the two must not share a lifecycle: redeploying a notebook
# should never be able to change a permission, and vice versa.
#
# Run it from this directory, never from the repo root.
# ==============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }

  # ----------------------------------------------------------------------------
  # REMOTE STATE IS REQUIRED BEFORE THIS IS TRUSTWORTHY — see the note below and
  # docs/UNITY_CATALOG_GRANTS.md ("What state buys you").
  #
  # `databricks_grants` is authoritative for each securable, so an apply always
  # converges the schemas it still names, with or without state. What state adds
  # is the memory of schemas the config USED to name: delete a domain from
  # model.tf with no state, and Terraform simply forgets the grants exist and
  # leaves them in Unity Catalog forever.
  #
  # So: local state is fine while the model only grows. The first removal is
  # where it silently stops being a model of reality.
  # ----------------------------------------------------------------------------
  # backend "s3" {
  #   bucket = "<state-bucket>"
  #   key    = "superstore/governance/terraform.tfstate"
  #   region = "<region>"
  # }
}

# ------------------------------------------------------------------------------
# Workspace-level provider. Grants on catalogs and schemas are workspace API
# calls, so this is deliberately NOT the account-level provider.
#
# Authentication comes from the environment (DATABRICKS_TOKEN, or a CLI profile
# via DATABRICKS_CONFIG_PROFILE) rather than from a variable, so a credential is
# never a value that could end up in state, a plan file or a PR comment.
#
# The identity used here must own the securables or be a metastore admin — it is
# the ONLY identity in this repo that needs that power, which is the entire
# reason this configuration is not part of the pipeline.
# ------------------------------------------------------------------------------
provider "databricks" {
  host = var.databricks_host
}
