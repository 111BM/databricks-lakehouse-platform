# ==============================================================================
# Inputs
# ==============================================================================
# Everything an operator might legitimately want to change without editing the
# model. The model itself — who reads which layer — lives in model.tf, because
# changing it is a policy decision and should show up in a diff as one.
# ==============================================================================

variable "databricks_host" {
  description = "Workspace URL that holds the Unity Catalog securables."
  type        = string
  default     = "https://dbc-081a6a55-88cd.cloud.databricks.com"
}

variable "catalog_name" {
  description = "The catalog holding every environment's schemas."
  type        = string
  default     = "superstore_catalog"
}

variable "environments" {
  description = <<-EOT
    Environments to govern. Each produces schemas named <env>_<domain>.

    `integration_test` is deliberately absent. That environment's schemas are
    created, corrupted by a seed and dropped by the test suite; standing human
    access to them would teach people that a data-quality violation there means
    something. It is machine-owned end to end.
  EOT
  type        = list(string)
  default     = ["dev", "qa", "prod"]

  validation {
    condition     = !contains(var.environments, "integration_test")
    error_message = "integration_test is machine-owned; granting humans standing access to it is not intended."
  }
}

variable "role_groups" {
  description = <<-EOT
    Role name -> the ACCOUNT group that holds it.

    Account groups, not workspace-local groups: a workspace-local group cannot
    be granted on a Unity Catalog securable. None of these exist in this account
    yet, so the first apply will fail on an unknown principal — loudly, which is
    the correct behaviour. See docs/UNITY_CATALOG_GRANTS.md ("Provisioning").

    The indirection exists so a group rename is one edit here rather than one
    per layer.
  EOT
  type        = map(string)

  default = {
    analysts        = "superstore_analysts"
    data_scientists = "superstore_data_scientists"
    engineers       = "superstore_engineers"
  }
}

variable "pipeline_service_principal" {
  description = <<-EOT
    Application ID of the service principal the pipeline runs as.

    Empty by default, and that is not an oversight: backlog item 1 (OAuth M2M
    service principal) has not landed, so the pipeline still runs as a person
    who OWNS these schemas and therefore needs no grant at all. Granting to a
    principal that does not exist would fail every apply.

    Set this when item 1 lands and the writer grants below start applying. That
    is the whole change — the privilege model for the writer is already written.
  EOT
  type        = string
  default     = ""
}
