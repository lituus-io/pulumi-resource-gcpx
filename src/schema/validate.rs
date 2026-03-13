use crate::schema::types::{
    has_reserved_prefix, is_valid_mode, is_valid_name, is_valid_type, AlterAction, CheckFailure,
    SchemaField,
};

/// Validate schema fields. Returns a list of check failures.
///
/// - `is_create`: true when this is a new resource (no old state).
/// - `old_fields`: previous schema fields from state, if any.
pub fn validate(
    fields: &[SchemaField<'_>],
    is_create: bool,
    old_fields: Option<&[SchemaField<'_>]>,
) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if fields.is_empty() {
        failures.push(CheckFailure {
            property: "schema".into(),
            reason: "schema must contain at least one field".into(),
        });
        return failures;
    }

    // Single-pass validation of each field.
    for (i, f) in fields.iter().enumerate() {
        let prefix = format!("schema[{}]", i);
        validate_field(f, &prefix, is_create, &mut failures);
    }

    // Cross-field: check for duplicate names (case-insensitive), excluding deleted.
    // Zero-alloc: O(n^2) with eq_ignore_ascii_case (schemas are <100 fields).
    let active: Vec<&str> = fields
        .iter()
        .filter(|f| f.alter != Some(AlterAction::Delete))
        .map(|f| f.name)
        .collect();
    for i in 0..active.len() {
        for j in (i + 1)..active.len() {
            if active[i].eq_ignore_ascii_case(active[j]) {
                failures.push(CheckFailure {
                    property: "schema".into(),
                    reason: format!("duplicate column name '{}' (case-insensitive)", active[j])
                        .into(),
                });
            }
        }
    }

    // Cross-field consistency checks against old schema.
    if let Some(old) = old_fields {
        for (i, f) in fields.iter().enumerate() {
            let prefix = format!("schema[{}]", i);
            match f.alter {
                Some(AlterAction::Delete) => {
                    // Idempotent: if column already gone, silently skip.
                }
                Some(AlterAction::Insert) => {
                    // Idempotent: if column already exists, silently skip.
                }
                Some(AlterAction::Rename) => {
                    // Idempotent: if alter_from no longer exists, already renamed — skip.
                }
                None => {
                    // Unchanged column must exist in old schema.
                    if !old.iter().any(|o| o.name.eq_ignore_ascii_case(f.name)) {
                        failures.push(CheckFailure {
                            property: format!("{}.name", prefix).into(),
                            reason: format!(
                                "column '{}' does not exist in current schema (use alter='insert' for new columns)",
                                f.name,
                            ).into(),
                        });
                    }
                }
            }
        }
    }

    failures
}

fn validate_field(
    f: &SchemaField<'_>,
    prefix: &str,
    is_create: bool,
    failures: &mut Vec<CheckFailure>,
) {
    // Name validation.
    if !is_valid_name(f.name) {
        failures.push(CheckFailure {
            property: format!("{}.name", prefix).into(),
            reason: format!(
                "invalid column name '{}': must match [a-zA-Z_][a-zA-Z0-9_]{{0,299}}",
                f.name,
            )
            .into(),
        });
    }
    if has_reserved_prefix(f.name) {
        failures.push(CheckFailure {
            property: format!("{}.name", prefix).into(),
            reason: format!("column name '{}' uses a reserved prefix", f.name).into(),
        });
    }

    // Type validation.
    if f.raw_type.is_empty() {
        failures.push(CheckFailure {
            property: format!("{}.type", prefix).into(),
            reason: "type must not be empty".into(),
        });
    } else if !is_valid_type(f.raw_type) {
        failures.push(CheckFailure {
            property: format!("{}.type", prefix).into(),
            reason: format!("invalid BigQuery type '{}'", f.raw_type).into(),
        });
    }

    // Mode validation.
    if !f.mode.is_empty() && !is_valid_mode(f.mode) {
        failures.push(CheckFailure {
            property: format!("{}.mode", prefix).into(),
            reason: format!(
                "invalid mode '{}': must be NULLABLE, REQUIRED, or REPEATED",
                f.mode,
            )
            .into(),
        });
    }

    // REQUIRED not allowed with alter=insert (BQ doesn't allow adding REQUIRED columns).
    if f.alter == Some(AlterAction::Insert) && f.mode.eq_ignore_ascii_case("REQUIRED") {
        failures.push(CheckFailure {
            property: format!("{}.mode", prefix).into(),
            reason: "cannot insert a REQUIRED column; BigQuery only allows NULLABLE or REPEATED for new columns".into(),
        });
    }

    // Alter value validation.
    if let Some(raw) = f.alter_raw {
        if f.alter.is_none() {
            failures.push(CheckFailure {
                property: format!("{}.alter", prefix).into(),
                reason: format!(
                    "invalid alter value '{}': must be 'rename', 'insert', or 'delete'",
                    raw
                )
                .into(),
            });
        }
    }

    // Rename/delete not valid on create.
    if is_create {
        match f.alter {
            Some(AlterAction::Rename) => {
                failures.push(CheckFailure {
                    property: format!("{}.alter", prefix).into(),
                    reason: "alter='rename' is not valid when creating a new resource".into(),
                });
            }
            Some(AlterAction::Delete) => {
                failures.push(CheckFailure {
                    property: format!("{}.alter", prefix).into(),
                    reason: "alter='delete' is not valid when creating a new resource".into(),
                });
            }
            _ => {}
        }
    }

    // alterFrom required for rename, invalid without rename.
    if f.alter == Some(AlterAction::Rename) && f.alter_from.is_none() {
        failures.push(CheckFailure {
            property: format!("{}.alterFrom", prefix).into(),
            reason: "alterFrom is required when alter='rename'".into(),
        });
    }
    if f.alter_from.is_some() && f.alter != Some(AlterAction::Rename) {
        failures.push(CheckFailure {
            property: format!("{}.alterFrom", prefix).into(),
            reason: "alterFrom is only valid when alter='rename'".into(),
        });
    }

    // roundingMode only valid on NUMERIC/BIGNUMERIC.
    if let Some(rm) = f.rounding_mode {
        if !matches!(f.canonical_type, "NUMERIC" | "BIGNUMERIC") {
            failures.push(CheckFailure {
                property: format!("{}.roundingMode", prefix).into(),
                reason: "roundingMode is only valid for NUMERIC or BIGNUMERIC columns".into(),
            });
        }
        if !matches!(rm, "ROUND_HALF_AWAY_FROM_ZERO" | "ROUND_HALF_EVEN") {
            failures.push(CheckFailure {
                property: format!("{}.roundingMode", prefix).into(),
                reason: "roundingMode must be ROUND_HALF_AWAY_FROM_ZERO or ROUND_HALF_EVEN".into(),
            });
        }
    }

    // RECORD/STRUCT must have nested fields.
    if f.canonical_type == "STRUCT" && f.fields.is_empty() {
        failures.push(CheckFailure {
            property: format!("{}.fields", prefix).into(),
            reason: "STRUCT/RECORD type requires at least one nested field".into(),
        });
    }

    // Recursive validation for nested fields.
    for (j, nested) in f.fields.iter().enumerate() {
        let nested_prefix = format!("{}.fields[{}]", prefix, j);
        validate_field(nested, &nested_prefix, is_create, failures);
    }
}

/// Consumer constraints from downstream Models/Snapshots — columns that must not be dropped.
pub struct ConsumerConstraints<'a> {
    pub unique_keys: Vec<&'a str>,
    pub partition_fields: Vec<&'a str>,
    pub cluster_columns: Vec<&'a str>,
}

/// Validate that columns being dropped don't conflict with downstream resource config.
pub fn validate_schema_drop_safety(
    drops: &[&str],
    constraints: &ConsumerConstraints<'_>,
) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    for &col in drops {
        if constraints
            .unique_keys
            .iter()
            .any(|k| k.eq_ignore_ascii_case(col))
        {
            failures.push(CheckFailure {
                property: "consumerConstraints.uniqueKeys".into(),
                reason: format!(
                    "cannot drop column '{}' — it is a unique_key for a downstream Model/Snapshot. \
                     Remove the alter='delete' or change the consumer's unique_key first.",
                    col
                )
                .into(),
            });
        }
        if constraints
            .partition_fields
            .iter()
            .any(|k| k.eq_ignore_ascii_case(col))
        {
            failures.push(CheckFailure {
                property: "consumerConstraints.partitionFields".into(),
                reason: format!(
                    "cannot drop column '{}' — it is a partition key for a downstream Model. \
                     Remove partitioning from the model config before dropping this column.",
                    col
                )
                .into(),
            });
        }
        if constraints
            .cluster_columns
            .iter()
            .any(|k| k.eq_ignore_ascii_case(col))
        {
            failures.push(CheckFailure {
                property: "consumerConstraints.clusterColumns".into(),
                reason: format!(
                    "cannot drop column '{}' — it is a clustering column for a downstream Model. \
                     Remove '{}' from cluster_by before dropping.",
                    col, col
                )
                .into(),
            });
        }
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::normalize_type;

    fn field<'a>(name: &'a str, ty: &'a str) -> SchemaField<'a> {
        SchemaField {
            name,
            raw_type: ty,
            canonical_type: normalize_type(ty),
            mode: "NULLABLE",
            description: "",
            alter: None,
            alter_raw: None,
            alter_from: None,
            default_value_expression: None,
            rounding_mode: None,
            fields: vec![],
        }
    }

    #[test]
    fn valid_basic_schema() {
        let fields = vec![field("col_a", "STRING"), field("col_b", "INT64")];
        let failures = validate(&fields, true, None);
        assert!(
            failures.is_empty(),
            "expected no failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_schema() {
        let failures = validate(&[], true, None);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("at least one field"));
    }

    #[test]
    fn invalid_name() {
        let fields = vec![field("123bad", "STRING")];
        let failures = validate(&fields, true, None);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("invalid column name")));
    }

    #[test]
    fn invalid_type() {
        let fields = vec![field("col", "BADTYPE")];
        let failures = validate(&fields, true, None);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("invalid BigQuery type")));
    }

    #[test]
    fn duplicate_names() {
        let fields = vec![field("Col_A", "STRING"), field("col_a", "INT64")];
        let failures = validate(&fields, true, None);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("duplicate column name")));
    }

    #[test]
    fn rename_on_create() {
        let mut f = field("new_col", "STRING");
        f.alter = Some(AlterAction::Rename);
        f.alter_raw = Some("rename");
        f.alter_from = Some("old_col");
        let failures = validate(&[f], true, None);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("not valid when creating")));
    }

    #[test]
    fn insert_required_rejected() {
        let mut f = field("new_col", "STRING");
        f.alter = Some(AlterAction::Insert);
        f.alter_raw = Some("insert");
        f.mode = "REQUIRED";
        let failures = validate(&[f], false, Some(&[]));
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("cannot insert a REQUIRED column")));
    }

    #[test]
    fn rename_missing_alter_from() {
        let mut f = field("new_col", "STRING");
        f.alter = Some(AlterAction::Rename);
        f.alter_raw = Some("rename");
        let failures = validate(&[f], false, Some(&[field("old_col", "STRING")]));
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("alterFrom is required")));
    }

    #[test]
    fn delete_nonexistent_column_is_idempotent() {
        // Declarative idempotency: deleting an already-gone column is a no-op.
        let mut f = field("ghost", "STRING");
        f.alter = Some(AlterAction::Delete);
        f.alter_raw = Some("delete");
        let failures = validate(&[f], false, Some(&[field("other", "STRING")]));
        assert!(
            failures.is_empty(),
            "expected no failures for idempotent delete: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn insert_already_existing_column_is_idempotent() {
        // Declarative idempotency: inserting an already-present column is a no-op.
        let mut f = field("col_a", "STRING");
        f.alter = Some(AlterAction::Insert);
        f.alter_raw = Some("insert");
        let failures = validate(&[f], false, Some(&[field("col_a", "STRING")]));
        assert!(
            failures.is_empty(),
            "expected no failures for idempotent insert: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn rename_already_applied_is_idempotent() {
        // Declarative idempotency: rename where alter_from no longer exists is a no-op.
        let mut f = field("new_name", "STRING");
        f.alter = Some(AlterAction::Rename);
        f.alter_raw = Some("rename");
        f.alter_from = Some("old_name");
        // old schema has new_name (already renamed), not old_name
        let failures = validate(&[f], false, Some(&[field("new_name", "STRING")]));
        assert!(
            failures.is_empty(),
            "expected no failures for idempotent rename: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    // ── Drop safety tests ──────────────────────────────────────────

    #[test]
    fn drop_safety_blocks_unique_key() {
        let constraints = ConsumerConstraints {
            unique_keys: vec!["customer_id"],
            partition_fields: vec![],
            cluster_columns: vec![],
        };
        let failures = validate_schema_drop_safety(&["customer_id"], &constraints);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("unique_key"));
    }

    #[test]
    fn drop_safety_blocks_partition_field() {
        let constraints = ConsumerConstraints {
            unique_keys: vec![],
            partition_fields: vec!["order_date"],
            cluster_columns: vec![],
        };
        let failures = validate_schema_drop_safety(&["order_date"], &constraints);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("partition key"));
    }

    #[test]
    fn drop_safety_blocks_cluster_column() {
        let constraints = ConsumerConstraints {
            unique_keys: vec![],
            partition_fields: vec![],
            cluster_columns: vec!["region"],
        };
        let failures = validate_schema_drop_safety(&["region"], &constraints);
        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("clustering column"));
    }

    #[test]
    fn drop_safety_allows_non_constrained() {
        let constraints = ConsumerConstraints {
            unique_keys: vec!["id"],
            partition_fields: vec!["ts"],
            cluster_columns: vec!["region"],
        };
        let failures = validate_schema_drop_safety(&["old_col"], &constraints);
        assert!(failures.is_empty());
    }

    #[test]
    fn drop_safety_multiple_violations() {
        let constraints = ConsumerConstraints {
            unique_keys: vec!["id"],
            partition_fields: vec!["id"],
            cluster_columns: vec!["id"],
        };
        let failures = validate_schema_drop_safety(&["id"], &constraints);
        assert_eq!(failures.len(), 3);
    }
}
