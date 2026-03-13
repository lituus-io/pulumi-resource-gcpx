use std::collections::BTreeMap;

use crate::bq::BqField;
use crate::dbt::macros::{expand_macros, MacroDef};
use crate::dbt::options::OwnedTableOptions;
use crate::dbt::scanner::{DbtScanner, DbtSegment};
use crate::dbt::types::{MacroInputs, ModelConfig, ModelRefData, ProjectContext, ProjectInputs};
use crate::schema::types::normalize_type;
use crate::schema::types::CheckFailure;

const RESERVED_NAMES: &[&str] = &["config", "ref", "source"];

pub fn validate_project(inputs: &ProjectInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if inputs.gcp_project.is_empty() {
        failures.push(CheckFailure {
            property: "gcpProject".into(),
            reason: "gcpProject cannot be empty: provide the target GCP project ID (e.g., 'analytics-prod')".into(),
        });
    }
    if inputs.dataset.is_empty() {
        failures.push(CheckFailure {
            property: "dataset".into(),
            reason: "dataset cannot be empty: provide the target BigQuery dataset where models will be created".into(),
        });
    }

    for (name, src) in &inputs.sources {
        if src.dataset.is_empty() {
            failures.push(CheckFailure {
                property: format!("sources.{}.dataset", name).into(),
                reason: format!(
                    "source '{}' dataset must not be empty: provide the dataset to read from",
                    name
                )
                .into(),
            });
        }
        if src.tables.is_empty() {
            failures.push(CheckFailure {
                property: format!("sources.{}.tables", name).into(),
                reason: format!("source '{}' must have at least one table: define tables that dbt can reference via source('{}', 'table_name')", name, name).into(),
            });
        }
    }

    // Unique model names.
    let mut seen_models = Vec::new();
    for m in &inputs.declared_models {
        if seen_models.iter().any(|s: &&str| s.eq_ignore_ascii_case(m)) {
            failures.push(CheckFailure {
                property: "declaredModels".into(),
                reason: format!(
                    "duplicate model name '{}' (case-insensitive) — each model name must be unique",
                    m
                )
                .into(),
            });
        }
        seen_models.push(m);
    }

    // Unique macro names.
    let mut seen_macros = Vec::new();
    for m in &inputs.declared_macros {
        if seen_macros.iter().any(|s: &&str| s.eq_ignore_ascii_case(m)) {
            failures.push(CheckFailure {
                property: "declaredMacros".into(),
                reason: format!("duplicate macro name '{}'", m).into(),
            });
        }
        if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(m)) {
            failures.push(CheckFailure {
                property: "declaredMacros".into(),
                reason: format!("macro name '{}' is reserved", m).into(),
            });
        }
        seen_macros.push(m);
    }

    // No collisions between models and macros.
    for m in &inputs.declared_models {
        if inputs
            .declared_macros
            .iter()
            .any(|mac| mac.eq_ignore_ascii_case(m))
        {
            failures.push(CheckFailure {
                property: "declaredModels".into(),
                reason: format!("name '{}' collides between models and macros", m).into(),
            });
        }
    }

    failures
}

pub fn validate_macro(inputs: &MacroInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if inputs.name.is_empty() {
        failures.push(CheckFailure {
            property: "name".into(),
            reason: "macro name must not be empty".into(),
        });
    }
    if inputs.sql.is_empty() {
        failures.push(CheckFailure {
            property: "sql".into(),
            reason: "macro sql must not be empty".into(),
        });
    }

    // Reserved names.
    if RESERVED_NAMES
        .iter()
        .any(|r| r.eq_ignore_ascii_case(inputs.name))
    {
        failures.push(CheckFailure {
            property: "name".into(),
            reason: format!("macro name '{}' is reserved", inputs.name).into(),
        });
    }

    // Unique arg names.
    let mut seen = Vec::new();
    for a in &inputs.args {
        if seen.iter().any(|s: &&str| s.eq_ignore_ascii_case(a)) {
            failures.push(CheckFailure {
                property: "args".into(),
                reason: format!(
                    "duplicate argument name '{}' — each argument name must be unique",
                    a
                )
                .into(),
            });
        }
        if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(a)) {
            failures.push(CheckFailure {
                property: "args".into(),
                reason: format!(
                    "argument name '{}' is reserved — cannot use config, ref, or source as argument names",
                    a
                ).into(),
            });
        }
        seen.push(a);
    }

    failures
}

/// Validates the `maxBytesBilled` input, if set.
pub fn validate_max_bytes_billed(max_bytes_billed: Option<i64>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();
    if let Some(limit) = max_bytes_billed {
        if limit <= 0 {
            failures.push(CheckFailure {
                property: "maxBytesBilled".into(),
                reason: format!(
                    "maxBytesBilled must be greater than 0, got {}. \
                     Set a positive byte limit or remove the field to disable cost governance.",
                    limit
                )
                .into(),
            });
        }
    }
    failures
}

pub fn validate_model(
    name: &str,
    sql: &str,
    config: &ModelConfig,
    context: &ProjectContext<'_>,
    model_refs: &BTreeMap<String, ModelRefData>,
    macros: &BTreeMap<String, MacroDef>,
) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if name.is_empty() {
        failures.push(CheckFailure {
            property: "name".into(),
            reason: "model name cannot be empty: provide a unique model identifier".into(),
        });
    }
    if sql.is_empty() {
        failures.push(CheckFailure {
            property: "sql".into(),
            reason:
                "model sql cannot be empty: provide the SELECT statement that defines the model"
                    .into(),
        });
    }

    // Model must be in project's declaredModels (when list is non-empty).
    if !context.declared_models.is_empty() && !context.declared_models.iter().any(|m| m == name) {
        failures.push(CheckFailure {
            property: "name".into(),
            reason: format!("model '{}' is not in project's declaredModels", name).into(),
        });
    }

    // Expand macros first for validation.
    let expanded = match expand_macros(sql, macros) {
        Ok(s) => s,
        Err(e) => {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: format!("macro expansion failed: {}", e).into(),
            });
            return failures;
        }
    };

    // Scan expanded SQL for refs/sources/macros and config block count.
    let mut config_count = 0;
    let mut has_materialized_key = false;

    for segment in DbtScanner::new(&expanded) {
        match segment {
            DbtSegment::Config { raw_args } => {
                config_count += 1;
                // Check if materialized key is present.
                for entry in crate::dbt::scanner::ConfigArgIter::new(raw_args) {
                    if entry.key == "materialized" {
                        has_materialized_key = true;
                    }
                }
            }
            DbtSegment::Ref { model } => {
                if !context.declared_models.iter().any(|m| m == model) {
                    let available: Vec<&str> =
                        context.declared_models.iter().map(|s| s.as_str()).collect();
                    failures.push(CheckFailure {
                        property: "sql".into(),
                        reason: format!(
                            "ref('{}') references unknown model — available models: {:?}. Add '{}' to the project's declaredModels list.",
                            model, available, model
                        ).into(),
                    });
                }
                if !model_refs.contains_key(model)
                    && context.declared_models.iter().any(|m| m == model)
                {
                    failures.push(CheckFailure {
                        property: "modelRefs".into(),
                        reason: format!(
                            "model '{}' is declared in project but not wired — add modelRefs.{}: ${{resourceName.modelOutput}}",
                            model, model
                        ).into(),
                    });
                }
            }
            DbtSegment::Source { source, table } => {
                if !context.sources.contains_key(source) {
                    let available: Vec<&str> = context.sources.keys().map(|s| s.as_str()).collect();
                    failures.push(CheckFailure {
                        property: "sql".into(),
                        reason: format!(
                            "unknown source '{}' — available sources: {:?}. Add '{}' to project sources.",
                            source, available, source
                        ).into(),
                    });
                } else if let Some(src) = context.sources.get(source) {
                    if !src.tables.iter().any(|t| t == table) {
                        let available: Vec<&str> = src.tables.iter().map(|s| s.as_str()).collect();
                        failures.push(CheckFailure {
                            property: "sql".into(),
                            reason: format!(
                                "source '{}' does not declare table '{}' — available tables in '{}': {:?}",
                                source, table, source, available
                            ).into(),
                        });
                    }
                }
            }
            DbtSegment::Call {
                name: call_name, ..
            } => {
                if !macros.contains_key(call_name) {
                    let available: Vec<&str> = macros.keys().map(|s| s.as_str()).collect();
                    failures.push(CheckFailure {
                        property: "sql".into(),
                        reason: format!(
                            "unknown macro '{}' — available macros: {:?}. Register the macro via the macros property.",
                            call_name, available
                        ).into(),
                    });
                }
            }
            DbtSegment::Sql(s) => {
                if s.contains("__dbt__cte__") {
                    failures.push(CheckFailure {
                        property: "sql".into(),
                        reason: "'__dbt__cte__' prefix is reserved for internal use".into(),
                    });
                }
            }
        }
    }

    if config_count > 1 {
        failures.push(CheckFailure {
            property: "sql".into(),
            reason: "model SQL must contain at most one {{ config(...) }} block".into(),
        });
    }

    // Use pre-extracted config for validation.
    let mat = &config.materialization;

    if config_count >= 1 && has_materialized_key {
        if !matches!(
            mat.as_str(),
            "table" | "view" | "materialized_view" | "ephemeral" | "incremental"
        ) {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: format!(
                    "invalid materialization '{}' — valid options: table, view, materialized_view, ephemeral, incremental",
                    mat
                ).into(),
            });
        }
        if mat == "incremental" && config.unique_key.is_none() && config.unique_key_list.is_none() {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: "incremental materialization requires unique_key in config: add unique_key='column_name' or change materialization".into(),
            });
        }
        if let Some(ref strategy) = config.incremental_strategy {
            if !matches!(strategy.as_str(), "merge" | "delete+insert" | "append") {
                failures.push(CheckFailure {
                    property: "sql".into(),
                    reason: format!(
                        "invalid incremental_strategy '{}' — valid options: merge, delete+insert, append",
                        strategy
                    ).into(),
                });
            }
        }
        if let Some(ref part) = config.partition_by {
            if part.field.is_empty() {
                failures.push(CheckFailure {
                    property: "sql".into(),
                    reason: "partition_by must include a 'field' key: e.g., partition_by={'field': 'created_at', 'data_type': 'date'}".into(),
                });
            }
        }
        if let Some(ref cols) = config.cluster_by {
            if cols.len() > 4 {
                failures.push(CheckFailure {
                    property: "sql".into(),
                    reason: format!(
                        "cluster_by has {} columns but BigQuery allows maximum 4",
                        cols.len()
                    )
                    .into(),
                });
            }
        }
        // OPTIONS validation.
        if config.require_partition_filter == Some(true) && config.partition_by.is_none() {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: "require_partition_filter=true requires partition_by to be set".into(),
            });
        }
        if config.partition_expiration_days.is_some() && config.partition_by.is_none() {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: "partition_expiration_days requires partition_by to be set".into(),
            });
        }
        if config.enable_refresh == Some(true) && mat != "materialized_view" {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: "enable_refresh is only valid for materialized_view materialization".into(),
            });
        }
        if config.refresh_interval_minutes.is_some() && config.enable_refresh != Some(true) {
            failures.push(CheckFailure {
                property: "sql".into(),
                reason: "refresh_interval_minutes requires enable_refresh=true".into(),
            });
        }
    } else if config_count == 1 && !has_materialized_key {
        failures.push(CheckFailure {
            property: "sql".into(),
            reason: "config() block missing materialized key: add materialized='table|view|incremental|ephemeral|materialized_view'".into(),
        });
    }

    failures
}

/// Validate model output columns against config constraints and optional declared schema.
///
/// `model_columns` comes from `dry_run_query()` schema output.
/// `declared_schema` comes from `get_table_schema()` when a tableId is provided.
pub fn validate_model_schema(
    model_columns: &[BqField],
    declared_schema: Option<&[BqField]>,
    config: &ModelConfig,
) -> Vec<String> {
    let mut errors = Vec::new();
    let col_names: Vec<&str> = model_columns.iter().map(|c| c.name.as_str()).collect();
    let available = col_names.join(", ");

    // ── 1. Config constraint validation ──────────────────────────
    if let Some(ref key) = config.unique_key {
        if !model_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(key))
        {
            errors.push(format!(
                "unique_key '{}' not found in model output columns. Available: [{}]",
                key, available
            ));
        }
    }

    if let Some(ref keys) = config.unique_key_list {
        for key in keys {
            if !model_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(key))
            {
                errors.push(format!(
                    "composite unique_key column '{}' not found in model output columns. Available: [{}]",
                    key, available
                ));
            }
        }
    }

    if let Some(ref part) = config.partition_by {
        if !model_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&part.field))
        {
            errors.push(format!(
                "partition_by field '{}' not found in model output columns. Available: [{}]",
                part.field, available
            ));
        }
    }

    if let Some(ref cols) = config.cluster_by {
        for col in cols {
            if !model_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(col))
            {
                errors.push(format!(
                    "cluster_by column '{}' not found in model output columns. Available: [{}]",
                    col, available
                ));
            }
        }
    }

    // ── 2. Schema contract validation (only with declared schema) ─
    if let Some(schema) = declared_schema {
        // Model columns not in schema → error (undeclared column).
        for mc in model_columns {
            if !schema.iter().any(|s| s.name.eq_ignore_ascii_case(&mc.name)) {
                errors.push(format!(
                    "Model produces column '{}' (type {}) not declared in schema. \
                     Add it to TableSchema or remove from SQL.",
                    mc.name, mc.field_type
                ));
            }
        }

        // Schema columns not in model → error (unless alter: insert/delete).
        for sf in schema {
            if matches!(sf.mode.to_uppercase().as_str(), "REPEATED") {
                // Nested/repeated fields may not appear as top-level columns.
                continue;
            }
            // alter: insert columns are added by TableSchema after model writes.
            if sf.description.contains("__gcpx_alter_insert__") {
                continue;
            }
            if !model_columns
                .iter()
                .any(|mc| mc.name.eq_ignore_ascii_case(&sf.name))
            {
                errors.push(format!(
                    "Schema declares column '{}' (type {}) but model SQL does not produce it.",
                    sf.name, sf.field_type
                ));
            }
        }

        // Type mismatches on overlapping columns.
        for mc in model_columns {
            if let Some(sf) = schema
                .iter()
                .find(|s| s.name.eq_ignore_ascii_case(&mc.name))
            {
                let model_type = normalize_type(&mc.field_type);
                let schema_type = normalize_type(&sf.field_type);
                if model_type != schema_type {
                    errors.push(format!(
                        "Column '{}' type mismatch: model produces {} but schema declares {}.",
                        mc.name, model_type, schema_type
                    ));
                }
            }
        }
    }

    errors
}

/// Compare SQL config options against YAML-declared options.
///
/// When YAML options are declared, they become the authoritative source.
/// Any SQL config option not also declared in YAML is an error (prevents silent drop).
/// Any value mismatch is an error with guidance that YAML overrides SQL.
pub fn diff_options(
    sql_config: &ModelConfig,
    yaml_options: &OwnedTableOptions,
) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    // Helper macro for Option<T> fields where T: Display + PartialEq
    macro_rules! diff_field {
        ($sql_field:expr, $yaml_field:expr, $sql_key:literal, $yaml_key:literal) => {
            match (&$sql_field, &$yaml_field) {
                (Some(sql_val), None) => {
                    failures.push(CheckFailure {
                        property: "options".into(),
                        reason: format!(
                            "SQL config option '{}' is set to '{}' but not declared in \
                             resource-level YAML options.\n\n  \
                             SQL:  {{{{ config({}={}) }}}}\n  \
                             YAML: (not declared)\n\n\
                             When resource-level YAML options are provided, all SQL config options must also be \
                             declared in YAML to prevent silent configuration conflicts.\n\n\
                             Add to your YAML:\n  options:\n    {}: {}\n\n\
                             Or remove '{}' from your SQL config block.",
                            $sql_key, sql_val, $sql_key, sql_val,
                            $yaml_key, sql_val, $sql_key,
                        ).into(),
                    });
                }
                (Some(sql_val), Some(yaml_val)) if sql_val != yaml_val => {
                    failures.push(CheckFailure {
                        property: "options".into(),
                        reason: format!(
                            "Option '{}' has conflicting values in SQL config and resource-level YAML options.\n\n  \
                             SQL config:  '{}'\n  \
                             YAML options: '{}'\n\n\
                             Resource-level YAML options override SQL config settings.\n\n\
                             To resolve, either:\n  \
                             1. Remove '{}' from your SQL config block (YAML value '{}' will be used)\n  \
                             2. Update the SQL config to match: {{{{ config({}='{}') }}}}",
                            $sql_key, sql_val, yaml_val,
                            $sql_key, yaml_val, $sql_key, yaml_val,
                        ).into(),
                    });
                }
                _ => {} // Both match, or SQL is None
            }
        };
    }

    diff_field!(
        sql_config.require_partition_filter,
        yaml_options.require_partition_filter,
        "require_partition_filter",
        "requirePartitionFilter"
    );
    diff_field!(
        sql_config.partition_expiration_days,
        yaml_options.partition_expiration_days,
        "partition_expiration_days",
        "partitionExpirationDays"
    );
    diff_field!(
        sql_config.friendly_name,
        yaml_options.friendly_name,
        "friendly_name",
        "friendlyName"
    );
    diff_field!(
        sql_config.description,
        yaml_options.description,
        "description",
        "description"
    );
    diff_field!(
        sql_config.kms_key_name,
        yaml_options.kms_key_name,
        "kms_key_name",
        "kmsKeyName"
    );
    diff_field!(
        sql_config.default_collation_name,
        yaml_options.default_collation_name,
        "default_collation_name",
        "defaultCollationName"
    );
    diff_field!(
        sql_config.enable_refresh,
        yaml_options.enable_refresh,
        "enable_refresh",
        "enableRefresh"
    );
    diff_field!(
        sql_config.refresh_interval_minutes,
        yaml_options.refresh_interval_minutes,
        "refresh_interval_minutes",
        "refreshIntervalMinutes"
    );
    diff_field!(
        sql_config.max_staleness,
        yaml_options.max_staleness,
        "max_staleness",
        "maxStaleness"
    );

    // Labels: special case — SQL labels are Vec<(String, String)>, YAML labels are BTreeMap.
    let sql_labels: BTreeMap<&str, &str> = sql_config
        .labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    for (key, sql_val) in &sql_labels {
        match yaml_options.labels.get(*key) {
            None => {
                failures.push(CheckFailure {
                    property: "options".into(),
                    reason: format!(
                        "Label '{}' is set to '{}' in SQL config but not declared in YAML labels.\n\n  \
                         SQL:  {{{{ config(labels={{'{key}': '{sql_val}'}}) }}}}\n  \
                         YAML: (label '{}' not declared)\n\n\
                         Add to your YAML:\n  options:\n    labels:\n      {}: {}",
                        key, sql_val, key, key, sql_val,
                    ).into(),
                });
            }
            Some(yaml_val) if yaml_val.as_str() != *sql_val => {
                failures.push(CheckFailure {
                    property: "options".into(),
                    reason: format!(
                        "Label '{}' has conflicting values:\n  \
                         SQL config:  '{}'\n  \
                         YAML options: '{}'\n\n\
                         Resource-level YAML options override SQL config settings.",
                        key, sql_val, yaml_val,
                    )
                    .into(),
                });
            }
            _ => {} // Match or YAML-only
        }
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbt::types::{PartitionConfig, SourceInputs};

    #[test]
    fn valid_project() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![(
                "raw",
                SourceInputs {
                    dataset: "raw_data",
                    tables: vec!["customers"],
                },
            )],
            declared_models: vec!["stg"],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures.is_empty());
    }

    #[test]
    fn empty_gcp_project() {
        let inputs = ProjectInputs {
            gcp_project: "",
            dataset: "ds",
            sources: vec![],
            declared_models: vec![],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("gcpProject")));
    }

    #[test]
    fn reserved_macro_name() {
        let inputs = MacroInputs {
            name: "ref",
            args: vec![],
            sql: "SELECT 1",
        };
        let failures = validate_macro(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("reserved")));
    }

    #[test]
    fn model_no_config_defaults_to_table() {
        let ctx = ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources: BTreeMap::new(),
            declared_models: vec!["m".to_owned()],
            declared_macros: vec![],
            vars: BTreeMap::new(),
        };
        let sql = "SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        // No config block is allowed — defaults to table materialization.
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn model_valid() {
        let ctx = ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources: BTreeMap::new(),
            declared_models: vec!["m".to_owned()],
            declared_macros: vec![],
            vars: BTreeMap::new(),
        };
        let sql = "{{ config(materialized='table') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn model_unknown_ref() {
        let ctx = ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources: BTreeMap::new(),
            declared_models: vec!["m".to_owned()],
            declared_macros: vec![],
            vars: BTreeMap::new(),
        };
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ ref('missing') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("references unknown model")));
    }

    #[test]
    fn model_dbt_cte_prefix_rejected() {
        let ctx = ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources: BTreeMap::new(),
            declared_models: vec!["m".to_owned()],
            declared_macros: vec![],
            vars: BTreeMap::new(),
        };
        let sql = "{{ config(materialized='table') }} WITH __dbt__cte__x AS (SELECT 1) SELECT * FROM __dbt__cte__x";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures.iter().any(|f| f.reason.contains("__dbt__cte__")));
    }

    // --- Additional edge cases ---

    fn make_ctx(models: &[&str], macros: &[&str]) -> ProjectContext<'static> {
        ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources: BTreeMap::new(),
            declared_models: models.iter().map(|s| s.to_string()).collect(),
            declared_macros: macros.iter().map(|s| s.to_string()).collect(),
            vars: BTreeMap::new(),
        }
    }

    fn make_ctx_with_sources(
        models: &[&str],
        sources: BTreeMap<String, crate::dbt::types::SourceDef>,
    ) -> ProjectContext<'static> {
        ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources,
            declared_models: models.iter().map(|s| s.to_string()).collect(),
            declared_macros: vec![],
            vars: BTreeMap::new(),
        }
    }

    #[test]
    fn empty_dataset_rejected() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "",
            sources: vec![],
            declared_models: vec![],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures.iter().any(|f| f.property == "dataset"));
    }

    #[test]
    fn source_empty_dataset_rejected() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![(
                "raw",
                SourceInputs {
                    dataset: "",
                    tables: vec!["t"],
                },
            )],
            declared_models: vec![],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.property.contains("sources.raw.dataset")));
    }

    #[test]
    fn source_empty_tables_rejected() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![(
                "raw",
                SourceInputs {
                    dataset: "raw_data",
                    tables: vec![],
                },
            )],
            declared_models: vec![],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.property.contains("sources.raw.tables")));
    }

    #[test]
    fn duplicate_model_case_insensitive() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![],
            declared_models: vec!["Stg", "stg"],
            declared_macros: vec![],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("duplicate model name")));
    }

    #[test]
    fn duplicate_macro_name() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![],
            declared_models: vec![],
            declared_macros: vec!["sk", "sk"],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("duplicate macro name")));
    }

    #[test]
    fn model_macro_name_collision() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![],
            declared_models: vec!["sk"],
            declared_macros: vec!["sk"],
            vars: vec![],
        };
        let failures = validate_project(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("collides")));
    }

    #[test]
    fn macro_empty_name_rejected() {
        let inputs = MacroInputs {
            name: "",
            args: vec![],
            sql: "SELECT 1",
        };
        let failures = validate_macro(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("must not be empty")));
    }

    #[test]
    fn macro_empty_sql_rejected() {
        let inputs = MacroInputs {
            name: "m",
            args: vec![],
            sql: "",
        };
        let failures = validate_macro(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("sql must not be empty")));
    }

    #[test]
    fn macro_duplicate_args() {
        let inputs = MacroInputs {
            name: "m",
            args: vec!["x", "x"],
            sql: "SELECT 1",
        };
        let failures = validate_macro(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("duplicate argument name")));
    }

    #[test]
    fn macro_reserved_arg_source() {
        let inputs = MacroInputs {
            name: "m",
            args: vec!["source"],
            sql: "SELECT 1",
        };
        let failures = validate_macro(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("reserved")));
    }

    #[test]
    fn model_not_in_declared_models() {
        let ctx = make_ctx(&["other"], &[]);
        let sql = "{{ config(materialized='table') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model(
            "missing_model",
            sql,
            &config,
            &ctx,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("not in project's declaredModels")));
    }

    #[test]
    fn model_ref_declared_but_not_wired() {
        let ctx = make_ctx(&["m", "stg"], &[]);
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ ref('stg') }}";
        let config = cfg(sql);
        let failures = validate_model(
            "m",
            sql,
            &config,
            &ctx,
            &BTreeMap::new(), // no modelRefs wired
            &BTreeMap::new(),
        );
        assert!(failures.iter().any(|f| f.reason.contains("not wired")));
    }

    #[test]
    fn model_source_table_not_declared() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "raw".to_owned(),
            crate::dbt::types::SourceDef {
                dataset: "raw_data".to_owned(),
                tables: vec!["customers".to_owned()],
            },
        );
        let ctx = make_ctx_with_sources(&["m"], sources);
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ source('raw', 'orders') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("does not declare table 'orders'")));
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("available tables")));
    }

    #[test]
    fn model_multiple_configs_rejected() {
        let ctx = make_ctx(&["m"], &[]);
        let sql = "{{ config(materialized='table') }} SELECT 1 {{ config(materialized='view') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures.iter().any(|f| f.reason.contains("at most one")));
    }

    #[test]
    fn model_no_materialization_in_config() {
        let ctx = make_ctx(&["m"], &[]);
        let sql = "{{ config(schema='raw') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("missing materialized key")));
    }

    #[test]
    fn model_invalid_materialization() {
        let ctx = make_ctx(&["m"], &[]);
        let sql = "{{ config(materialized='snapshot') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("invalid materialization 'snapshot'")));
        assert!(failures.iter().any(|f| f.reason.contains("valid options")));
    }

    #[test]
    fn model_incremental_valid_with_unique_key() {
        let ctx = make_ctx(&["m"], &[]);
        let sql = "{{ config(materialized='incremental', unique_key='id') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn model_incremental_missing_unique_key() {
        let ctx = make_ctx(&["m"], &[]);
        let sql = "{{ config(materialized='incremental') }} SELECT 1";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures.iter().any(|f| f.reason.contains("unique_key")));
    }

    #[test]
    fn model_unknown_macro_actionable_error() {
        let ctx = make_ctx(&["m"], &[]);
        let mut macros = BTreeMap::new();
        macros.insert(
            "upper_col".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "UPPER({{ x }})".to_owned(),
            },
        );
        let sql = "{{ config(materialized='table') }} SELECT {{ unknown_macro('x') }}";
        let config = cfg_m(sql, &macros);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &macros);
        assert!(failures.iter().any(|f| {
            f.reason.contains("unknown macro 'unknown_macro'") && f.reason.contains("upper_col")
        }));
    }

    #[test]
    fn model_unknown_source_actionable_error() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "staging".to_owned(),
            crate::dbt::types::SourceDef {
                dataset: "stg".to_owned(),
                tables: vec!["orders".to_owned()],
            },
        );
        let ctx = make_ctx_with_sources(&["m"], sources);
        let sql =
            "{{ config(materialized='table') }} SELECT * FROM {{ source('missing', 'orders') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures.iter().any(|f| {
            f.reason.contains("unknown source 'missing'") && f.reason.contains("staging")
        }));
    }

    #[test]
    fn model_unknown_ref_actionable_error() {
        let ctx = make_ctx(&["m", "stg", "mart"], &[]);
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ ref('foo') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &BTreeMap::new(), &BTreeMap::new());
        assert!(failures.iter().any(|f| {
            f.reason.contains("references unknown model")
                && f.reason.contains("stg")
                && f.reason.contains("mart")
        }));
    }

    #[test]
    fn model_wired_ref_valid() {
        let ctx = make_ctx(&["m", "stg"], &[]);
        let mut refs = BTreeMap::new();
        refs.insert(
            "stg".to_owned(),
            ModelRefData {
                materialization: "view".to_owned(),
                resolved_ctes_json: "[]".to_owned(),
                resolved_body: "SELECT 1".to_owned(),
                table_ref: "`p.d.stg`".to_owned(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ ref('stg') }}";
        let config = cfg(sql);
        let failures = validate_model("m", sql, &config, &ctx, &refs, &BTreeMap::new());
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn model_all_ref_types_combined() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "raw".to_owned(),
            crate::dbt::types::SourceDef {
                dataset: "raw_data".to_owned(),
                tables: vec!["customers".to_owned()],
            },
        );
        let ctx = ProjectContext {
            gcp_project: "p",
            dataset: "d",
            sources,
            declared_models: vec!["m".to_owned(), "stg".to_owned()],
            declared_macros: vec![],
            vars: BTreeMap::new(),
        };
        let mut refs = BTreeMap::new();
        refs.insert(
            "stg".to_owned(),
            ModelRefData {
                materialization: "view".to_owned(),
                resolved_ctes_json: "[]".to_owned(),
                resolved_body: "SELECT 1".to_owned(),
                table_ref: "`p.d.stg`".to_owned(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
        let mut macros = BTreeMap::new();
        macros.insert(
            "sk".to_owned(),
            MacroDef {
                args: vec!["cols".to_owned()],
                sql: "MD5({{ cols }})".to_owned(),
            },
        );
        let sql = "{{ config(materialized='table') }} SELECT {{ sk('id') }} FROM {{ ref('stg') }} JOIN {{ source('raw', 'customers') }}";
        let config = cfg_m(sql, &macros);
        let failures = validate_model("m", sql, &config, &ctx, &refs, &macros);
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    // ── validate_model_schema tests ────────────────────────────────

    fn bqf(name: &str, ft: &str) -> BqField {
        BqField {
            name: name.to_owned(),
            field_type: ft.to_owned(),
            mode: "NULLABLE".to_owned(),
            description: String::new(),
            fields: vec![],
        }
    }

    fn empty_config() -> ModelConfig {
        ModelConfig {
            materialization: "table".to_owned(),
            unique_key: None,
            unique_key_list: None,
            incremental_strategy: None,
            partition_by: None,
            cluster_by: None,
            require_partition_filter: None,
            partition_expiration_days: None,
            friendly_name: None,
            description: None,
            labels: vec![],
            kms_key_name: None,
            default_collation_name: None,
            enable_refresh: None,
            refresh_interval_minutes: None,
            max_staleness: None,
        }
    }

    /// Extract config from SQL for test convenience.
    fn cfg(sql: &str) -> ModelConfig {
        crate::dbt::handlers::extract_model_config(sql, &BTreeMap::new())
    }

    fn cfg_m(sql: &str, macros: &BTreeMap<String, MacroDef>) -> ModelConfig {
        crate::dbt::handlers::extract_model_config(sql, macros)
    }

    #[test]
    fn schema_validate_missing_unique_key() {
        let cols = vec![bqf("id", "INT64"), bqf("name", "STRING")];
        let mut cfg = empty_config();
        cfg.unique_key = Some("event_id".to_owned());
        let errors = validate_model_schema(&cols, None, &cfg);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("unique_key 'event_id'"));
        assert!(errors[0].contains("id, name"));
    }

    #[test]
    fn schema_validate_missing_partition_by() {
        let cols = vec![bqf("id", "INT64")];
        let mut cfg = empty_config();
        cfg.partition_by = Some(PartitionConfig {
            field: "created_at".to_owned(),
            data_type: "timestamp".to_owned(),
            granularity: None,
        });
        let errors = validate_model_schema(&cols, None, &cfg);
        assert!(errors
            .iter()
            .any(|e| e.contains("partition_by field 'created_at'")));
    }

    #[test]
    fn schema_validate_missing_cluster_by() {
        let cols = vec![bqf("id", "INT64")];
        let mut cfg = empty_config();
        cfg.cluster_by = Some(vec!["region".to_owned()]);
        let errors = validate_model_schema(&cols, None, &cfg);
        assert!(errors
            .iter()
            .any(|e| e.contains("cluster_by column 'region'")));
    }

    #[test]
    fn schema_validate_composite_unique_key() {
        let cols = vec![bqf("a", "INT64"), bqf("b", "STRING")];
        let mut cfg = empty_config();
        cfg.unique_key_list = Some(vec!["a".to_owned(), "c".to_owned()]);
        let errors = validate_model_schema(&cols, None, &cfg);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("composite unique_key column 'c'"));
    }

    #[test]
    fn schema_validate_model_col_not_in_schema() {
        let cols = vec![bqf("id", "INT64"), bqf("extra", "STRING")];
        let schema = vec![bqf("id", "INT64")];
        let errors = validate_model_schema(&cols, Some(&schema), &empty_config());
        assert!(errors
            .iter()
            .any(|e| e.contains("Model produces column 'extra'")));
    }

    #[test]
    fn schema_validate_schema_col_not_in_model() {
        let cols = vec![bqf("id", "INT64")];
        let schema = vec![bqf("id", "INT64"), bqf("full_name", "STRING")];
        let errors = validate_model_schema(&cols, Some(&schema), &empty_config());
        assert!(errors
            .iter()
            .any(|e| e.contains("Schema declares column 'full_name'")));
    }

    #[test]
    fn schema_validate_type_mismatch() {
        let cols = vec![bqf("id", "STRING")];
        let schema = vec![bqf("id", "INT64")];
        let errors = validate_model_schema(&cols, Some(&schema), &empty_config());
        assert!(errors
            .iter()
            .any(|e| e.contains("type mismatch") && e.contains("STRING") && e.contains("INT64")));
    }

    #[test]
    fn schema_validate_all_match() {
        let cols = vec![bqf("id", "INT64"), bqf("name", "STRING")];
        let schema = vec![bqf("id", "INT64"), bqf("name", "STRING")];
        let mut cfg = empty_config();
        cfg.unique_key = Some("id".to_owned());
        let errors = validate_model_schema(&cols, Some(&schema), &cfg);
        assert!(errors.is_empty(), "expected no errors: {:?}", errors);
    }

    #[test]
    fn schema_validate_no_schema_config_only() {
        let cols = vec![bqf("id", "INT64"), bqf("name", "STRING")];
        let mut cfg = empty_config();
        cfg.unique_key = Some("id".to_owned());
        cfg.cluster_by = Some(vec!["id".to_owned()]);
        let errors = validate_model_schema(&cols, None, &cfg);
        assert!(errors.is_empty());
    }

    #[test]
    fn schema_validate_multiple_errors() {
        let cols = vec![bqf("id", "STRING"), bqf("extra", "STRING")];
        let schema = vec![bqf("id", "INT64"), bqf("full_name", "STRING")];
        let mut cfg = empty_config();
        cfg.unique_key = Some("event_id".to_owned());
        let errors = validate_model_schema(&cols, Some(&schema), &cfg);
        // unique_key missing + model has extra + schema missing full_name + type mismatch on id
        assert!(errors.len() >= 3, "expected multiple errors: {:?}", errors);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn validate_model_schema_never_panics(
            col_name in "[a-z_]{1,20}",
            col_type in prop_oneof!["INT64", "STRING", "FLOAT64", "BOOL", "TIMESTAMP", "DATE"],
            key_name in "[a-z_]{1,20}",
            part_field in "[a-z_]{1,20}",
        ) {
            let cols = vec![bqf(&col_name, &col_type)];
            let schema = vec![bqf(&col_name, &col_type)];
            let mut cfg = empty_config();
            cfg.unique_key = Some(key_name);
            cfg.partition_by = Some(PartitionConfig {
                field: part_field,
                data_type: "timestamp".to_owned(),
                granularity: None,
            });
            // Should never panic — only return error messages
            let _ = std::hint::black_box(validate_model_schema(&cols, Some(&schema), &cfg));
            let _ = std::hint::black_box(validate_model_schema(&cols, None, &cfg));
        }

        #[test]
        fn validate_model_never_panics(sql in "[ -~]{0,300}") {
            let ctx = ProjectContext {
                gcp_project: "p",
                dataset: "d",
                sources: BTreeMap::new(),
                declared_models: vec!["m".to_owned()],
                declared_macros: vec![],
                vars: BTreeMap::new(),
            };
            let config = cfg(&sql);
            // Should never panic — only return check failures
            let _ = std::hint::black_box(validate_model(
                "m",
                &sql,
                &config,
                &ctx,
                &BTreeMap::new(),
                &BTreeMap::new(),
            ));
        }
    }

    // ── diff_options tests ───────────────────────────────────────

    use crate::dbt::options::OwnedTableOptions;

    fn sql_config_with(sql: &str) -> ModelConfig {
        cfg(sql)
    }

    #[test]
    fn diff_options_both_none_no_errors() {
        let config = empty_config();
        let yaml = OwnedTableOptions::default();
        let failures = diff_options(&config, &yaml);
        assert!(failures.is_empty());
    }

    #[test]
    fn diff_options_sql_none_yaml_some_ok() {
        let config = empty_config();
        let yaml = OwnedTableOptions {
            require_partition_filter: Some(true),
            friendly_name: Some("My Table".to_owned()),
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        assert!(
            failures.is_empty(),
            "YAML-only options should be OK: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn diff_options_both_identical_ok() {
        let config = sql_config_with(
            "{{ config(materialized='table', require_partition_filter=true, \
             partition_by={'field': 'ts', 'data_type': 'date'}, \
             description='Orders') }} SELECT 1",
        );
        let yaml = OwnedTableOptions {
            require_partition_filter: Some(true),
            description: Some("Orders".to_owned()),
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        assert!(
            failures.is_empty(),
            "identical options should be OK: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn diff_options_sql_only_error() {
        let config = sql_config_with(
            "{{ config(materialized='table', require_partition_filter=true, \
             partition_by={'field': 'ts', 'data_type': 'date'}) }} SELECT 1",
        );
        let yaml = OwnedTableOptions::default();
        let failures = diff_options(&config, &yaml);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("require_partition_filter"));
        assert!(failures[0].reason.contains("not declared"));
        assert!(failures[0].reason.contains("requirePartitionFilter"));
    }

    #[test]
    fn diff_options_mismatch_error() {
        let config = sql_config_with(
            "{{ config(materialized='table', description='Orders fact table') }} SELECT 1",
        );
        let yaml = OwnedTableOptions {
            description: Some("Customer orders".to_owned()),
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("conflicting values"));
        assert!(failures[0].reason.contains("Orders fact table"));
        assert!(failures[0].reason.contains("Customer orders"));
    }

    #[test]
    fn diff_options_labels_sql_only_error() {
        let config =
            sql_config_with("{{ config(materialized='table', labels={'env': 'prod'}) }} SELECT 1");
        let yaml = OwnedTableOptions::default();
        let failures = diff_options(&config, &yaml);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("Label 'env'"));
        assert!(failures[0].reason.contains("not declared"));
    }

    #[test]
    fn diff_options_labels_mismatch_error() {
        let config = sql_config_with(
            "{{ config(materialized='table', labels={'env': 'staging'}) }} SELECT 1",
        );
        let mut labels = BTreeMap::new();
        labels.insert("env".to_owned(), "prod".to_owned());
        let yaml = OwnedTableOptions {
            labels,
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("Label 'env'"));
        assert!(failures[0].reason.contains("staging"));
        assert!(failures[0].reason.contains("prod"));
    }

    #[test]
    fn diff_options_labels_yaml_only_ok() {
        let config = empty_config();
        let mut labels = BTreeMap::new();
        labels.insert("team".to_owned(), "analytics".to_owned());
        let yaml = OwnedTableOptions {
            labels,
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        assert!(failures.is_empty());
    }

    #[test]
    fn diff_options_multiple_errors() {
        let config = sql_config_with(
            "{{ config(materialized='table', require_partition_filter=true, \
             partition_by={'field': 'ts', 'data_type': 'date'}, \
             description='Old desc', labels={'env': 'prod'}) }} SELECT 1",
        );
        let yaml = OwnedTableOptions {
            description: Some("New desc".to_owned()),
            // Missing require_partition_filter and labels
            ..Default::default()
        };
        let failures = diff_options(&config, &yaml);
        // Should have errors for: require_partition_filter (sql-only), description (mismatch), label env (sql-only)
        assert!(
            failures.len() >= 3,
            "expected at least 3 failures, got {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }
}
