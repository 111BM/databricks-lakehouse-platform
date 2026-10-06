# ==============================================================================
# The permission model
# ==============================================================================
#
# Read this file as the answer to one question: who can read what, and why is
# that the smallest thing that works?
#
# The layer IS the schema here — superstore_catalog_and_schemas_init creates
# <env>_<domain> for nine domains — so a per-layer model is expressible entirely
# in schema-level grants. Nothing below is granted on a table. That matters
# operationally: privileges on a schema are inherited by the tables in it,
# including tables that do not exist yet, so adding a Gold fact next month
# requires no governance change at all.
#
# Privilege names use UNDERSCORES here (USE_SCHEMA) and SPACES in SQL
# (USE SCHEMA). The provider takes the underscore form.
#
# Reasoning per layer is in docs/UNITY_CATALOG_GRANTS.md. The comments here are
# the short version, kept next to the lines they explain.
# ==============================================================================

locals {

  # ----------------------------------------------------------------------------
  # Catalog-level privileges — TRAVERSAL ONLY.
  #
  # This is the rule the whole model rests on. dev, qa and prod are schemas in
  # ONE catalog, so a single `SELECT` here would hand every role every layer of
  # every environment at once. USE_CATALOG is a gate, not access: without it the
  # schema grants below are inert, and with it alone you can still read nothing.
  #
  # BROWSE additionally exposes the metadata — names, columns, comments — of
  # objects you cannot query. That is a discovery aid for people who operate the
  # platform and a small leak of quarantine structure for people who consume it,
  # so consumers do not get it.
  #
  # A precondition in main.tf fails the plan if anything but USE_CATALOG or
  # BROWSE ever appears here.
  # ----------------------------------------------------------------------------
  catalog_privileges = {
    analysts        = ["USE_CATALOG"]
    data_scientists = ["USE_CATALOG"]
    engineers       = ["USE_CATALOG", "BROWSE"]
  }

  # ----------------------------------------------------------------------------
  # Per-layer model, applied to <env>_<domain> for every governed environment.
  #
  # USE_SCHEMA appears explicitly on every entry rather than being implied.
  # SELECT without USE_SCHEMA is the single most common way a permission model
  # looks correct in review and denies access in the warehouse — the grant is
  # there, the traversal is not. A precondition in main.tf enforces the pairing.
  # ----------------------------------------------------------------------------
  model = {

    # Raw arrivals, one row per landing. Source column names, unvalidated values,
    # and customer rows carrying names and addresses. Consumers have no reason to
    # be here and every reason not to be.
    bronze = {
      engineers = ["USE_SCHEMA", "SELECT"]
    }

    # Cleaned and deduplicated — still row-level PII.
    silver = {
      engineers = ["USE_SCHEMA", "SELECT"]
    }

    # Rows rejected by a fatal data-quality rule: raw values, no validation, plus
    # the reject reason. The same PII as Bronze with none of the cleaning.
    quarantine = {
      engineers = ["USE_SCHEMA", "SELECT"]
    }

    # Duplicates removed by Silver dedup — raw row copies again, retained so the
    # reconciliation invariant can account for every Bronze arrival.
    audit = {
      engineers = ["USE_SCHEMA", "SELECT"]
    }

    # Per-run PIPELINE telemetry: silver_layer_metrics, gold_layer_metrics, run
    # counts, durations, run_status. Named "metrics" but operational, not
    # business — the business KPIs are views in semantic_layer. Analysts do not
    # get this one, and the name is the only reason that looks surprising.
    metrics = {
      engineers = ["USE_SCHEMA", "SELECT"]
    }

    # Conformed dimensions and facts. The modelled layer, and the first one a
    # consumer should ever touch.
    gold = {
      analysts        = ["USE_SCHEMA", "SELECT"]
      data_scientists = ["USE_SCHEMA", "SELECT"]
      engineers       = ["USE_SCHEMA", "SELECT"]
    }

    # Curated subject-area tables built from Gold.
    mart = {
      analysts        = ["USE_SCHEMA", "SELECT"]
      data_scientists = ["USE_SCHEMA", "SELECT"]
      engineers       = ["USE_SCHEMA", "SELECT"]
    }

    # Business-KPI views over the marts. The intended analyst surface.
    #
    # These are VIEWS, which is what makes the model work rather than merely
    # look tidy: while the views and the mart tables share an owner, SELECT on
    # the view is sufficient and no grant on the underlying tables is needed.
    # If ownership ever diverges, that stops being true — see the doc.
    semantic_layer = {
      analysts        = ["USE_SCHEMA", "SELECT"]
      data_scientists = ["USE_SCHEMA", "SELECT"]
      engineers       = ["USE_SCHEMA", "SELECT"]
    }

    # Model features. Wider and less curated than the marts: useful to whoever is
    # training something, noise to whoever is reporting.
    features = {
      data_scientists = ["USE_SCHEMA", "SELECT"]
      engineers       = ["USE_SCHEMA", "SELECT"]
    }
  }

  # ----------------------------------------------------------------------------
  # Write privileges.
  #
  # Held by exactly one principal, and only when it exists. A prod table changes
  # because the pipeline changed it — never because a human ran an UPDATE at 2am.
  #
  # semantic_layer additionally needs CREATE_FUNCTION: the KPI notebooks create
  # views and may create functions there.
  # ----------------------------------------------------------------------------
  writer_privileges = {
    default        = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
    semantic_layer = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE", "CREATE_FUNCTION"]
  }

  # The writer joins the model only once a service principal is configured. Until
  # the model can apply, the pipeline's service principals are granted by
  # governance/manual_grants/ instead.
  model_with_writer = {
    for domain, roles in local.model :
    domain => (
      var.pipeline_service_principal == ""
      ? roles
      : merge(roles, {
        pipeline = lookup(local.writer_privileges, domain, local.writer_privileges.default)
      })
    )
  }

  # All principals by role, including the writer when present.
  principals = merge(
    var.role_groups,
    var.pipeline_service_principal == "" ? {} : { pipeline = var.pipeline_service_principal },
  )

  # ----------------------------------------------------------------------------
  # Environment shaping.
  #
  # dev is where engineers are expected to create and drop tables by hand; prod
  # is where doing that by hand is the incident. So engineers get the writer's
  # privileges in dev and read-only everywhere else. Nothing in this file grants
  # MODIFY on prod to a human.
  #
  # The result is one map: "<env>_<domain>" => { role => privileges }.
  # ----------------------------------------------------------------------------
  schema_grants = {
    for pair in setproduct(var.environments, keys(local.model_with_writer)) :
    "${pair[0]}_${pair[1]}" => (
      pair[0] == "dev"
      ? merge(local.model_with_writer[pair[1]], {
        engineers = lookup(local.writer_privileges, pair[1], local.writer_privileges.default)
      })
      : local.model_with_writer[pair[1]]
    )
  }

  # Layers no consumer role may ever reach, in any environment. Asserted in
  # main.tf rather than merely observed above, so that adding `analysts` to
  # quarantine in a hurry fails the plan instead of passing review.
  consumer_restricted_domains = ["bronze", "silver", "quarantine", "audit", "metrics"]
  consumer_roles              = ["analysts", "data_scientists"]
}
