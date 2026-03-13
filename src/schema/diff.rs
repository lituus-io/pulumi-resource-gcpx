use crate::schema::types::{AlterAction, DdlOp, DiffResult, OwnedDdlOp, SchemaField};

/// Compute diff between old (clean, from state) and new (annotated) schema fields.
#[allow(clippy::too_many_arguments)]
pub fn compute_diff<'a>(
    old_project: &str,
    new_project: &str,
    old_dataset: &str,
    new_dataset: &str,
    old_table_id: &str,
    new_table_id: &str,
    old_fields: &[SchemaField<'_>],
    new_fields: &'a [SchemaField<'a>],
) -> DiffResult<'a> {
    let mut replace_keys = Vec::new();
    if old_project != new_project {
        replace_keys.push("project");
    }
    if old_dataset != new_dataset {
        replace_keys.push("dataset");
    }
    if old_table_id != new_table_id {
        replace_keys.push("tableId");
    }

    let mut drops = Vec::new();
    let mut renames = Vec::new();
    let mut adds = Vec::new();
    let mut descriptions = Vec::new();

    for f in new_fields {
        match f.alter {
            Some(AlterAction::Delete) => {
                // Idempotent: only drop if column still exists in old schema.
                if old_fields
                    .iter()
                    .any(|o| o.name.eq_ignore_ascii_case(f.name))
                {
                    drops.push(DdlOp::DropColumn { name: f.name });
                }
            }
            Some(AlterAction::Rename) => {
                if let Some(from) = f.alter_from {
                    // Idempotent: only rename if alter_from still exists in old schema.
                    if old_fields.iter().any(|o| o.name.eq_ignore_ascii_case(from)) {
                        renames.push(DdlOp::RenameColumn {
                            old_name: from,
                            new_name: f.name,
                        });
                    }
                }
            }
            Some(AlterAction::Insert) => {
                // Idempotent: only add if column doesn't already exist in old schema.
                if !old_fields
                    .iter()
                    .any(|o| o.name.eq_ignore_ascii_case(f.name))
                {
                    adds.push(DdlOp::AddColumn {
                        name: f.name,
                        field_type: f.canonical_type,
                        mode: f.mode,
                        description: f.description,
                        default_value_expression: f.default_value_expression,
                        rounding_mode: f.rounding_mode,
                    });
                }
            }
            None => {
                // Check if description changed.
                if let Some(old_f) = old_fields
                    .iter()
                    .find(|o| o.name.eq_ignore_ascii_case(f.name))
                {
                    if old_f.description != f.description {
                        descriptions.push(DdlOp::SetDescription {
                            name: f.name,
                            description: f.description,
                        });
                    }
                }
            }
        }
    }

    // Detect rename cycles and break them.
    let owned_ops = break_rename_cycles(&renames);

    // Order: drops, renames, adds, descriptions.
    let mut ops = Vec::new();
    ops.extend(drops);
    ops.extend(renames);
    ops.extend(adds);
    ops.extend(descriptions);

    let has_schema_changes = !ops.is_empty() || !owned_ops.is_empty();

    DiffResult {
        ops,
        owned_ops,
        replace_keys,
        has_schema_changes,
    }
}

/// Detect rename cycles and return temporary rename operations to break them.
///
/// Index-based: Vec<(&str, &str)> + visited: Vec<bool>.
pub fn break_rename_cycles(renames: &[DdlOp<'_>]) -> Vec<OwnedDdlOp> {
    // Collect rename pairs as (&str, &str).
    let pairs: Vec<(&str, &str)> = renames
        .iter()
        .filter_map(|op| match op {
            DdlOp::RenameColumn { old_name, new_name } => Some((*old_name, *new_name)),
            _ => None,
        })
        .collect();

    if pairs.is_empty() {
        return Vec::new();
    }

    let mut visited = vec![false; pairs.len()];
    let mut result = Vec::new();
    let mut tmp_counter = 0u32;

    for start_idx in 0..pairs.len() {
        if visited[start_idx] {
            continue;
        }

        // Walk the chain from start_idx.
        let mut chain = vec![start_idx];
        visited[start_idx] = true;
        let mut current_target = pairs[start_idx].1;

        loop {
            // Find the next rename whose old_name matches current_target.
            let next = pairs
                .iter()
                .enumerate()
                .find(|(idx, (old, _))| !visited[*idx] && old.eq_ignore_ascii_case(current_target));

            match next {
                Some((idx, _)) => {
                    visited[idx] = true;
                    chain.push(idx);
                    current_target = pairs[idx].1;
                }
                None => {
                    // Check if current_target cycles back to the start.
                    if current_target.eq_ignore_ascii_case(pairs[start_idx].0) && chain.len() > 1 {
                        // Cycle detected! Break it.
                        let tmp_name = format!("__gcpx_tmp_{}", tmp_counter);
                        tmp_counter += 1;

                        // Step 1: first_old -> tmp
                        result.push(OwnedDdlOp::RenameColumn {
                            old_name: pairs[start_idx].0.to_owned(),
                            new_name: tmp_name.clone(),
                        });

                        // Step 2: tmp -> first_new (the target of first rename)
                        result.push(OwnedDdlOp::RenameColumn {
                            old_name: tmp_name,
                            new_name: pairs[start_idx].1.to_owned(),
                        });
                    }
                    break;
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ddl::build_batch_ddl;
    use crate::schema::types::{clean_fields, normalize_type};

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
    fn no_changes() {
        let old = vec![field("col_a", "STRING")];
        let new = vec![field("col_a", "STRING")];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(!diff.has_schema_changes);
        assert!(diff.ops.is_empty());
    }

    #[test]
    fn detect_insert() {
        let old = vec![field("col_a", "STRING")];
        let mut ins = field("col_b", "INT64");
        ins.alter = Some(AlterAction::Insert);
        let new = vec![field("col_a", "STRING"), ins];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(diff.has_schema_changes);
        assert!(matches!(
            diff.ops.last(),
            Some(DdlOp::AddColumn { name: "col_b", .. })
        ));
    }

    #[test]
    fn detect_delete() {
        let old = vec![field("col_a", "STRING"), field("col_b", "INT64")];
        let mut del = field("col_b", "INT64");
        del.alter = Some(AlterAction::Delete);
        let new = vec![field("col_a", "STRING"), del];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(diff.has_schema_changes);
        assert!(matches!(
            diff.ops.first(),
            Some(DdlOp::DropColumn { name: "col_b" })
        ));
    }

    #[test]
    fn detect_rename() {
        let old = vec![field("old_name", "STRING")];
        let mut ren = field("new_name", "STRING");
        ren.alter = Some(AlterAction::Rename);
        ren.alter_from = Some("old_name");
        let new = vec![ren];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(diff.has_schema_changes);
        assert!(matches!(
            diff.ops.first(),
            Some(DdlOp::RenameColumn {
                old_name: "old_name",
                new_name: "new_name"
            })
        ));
    }

    #[test]
    fn detect_description_change() {
        let mut old = field("col_a", "STRING");
        old.description = "old desc";
        let mut new = field("col_a", "STRING");
        new.description = "new desc";
        let old_arr = [old];
        let new_arr = [new];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old_arr, &new_arr);
        assert!(diff.has_schema_changes);
        assert!(matches!(
            diff.ops.first(),
            Some(DdlOp::SetDescription {
                name: "col_a",
                description: "new desc"
            })
        ));
    }

    #[test]
    fn replace_on_project_change() {
        let old = vec![field("col", "STRING")];
        let new = vec![field("col", "STRING")];
        let diff = compute_diff("p1", "p2", "d", "d", "t", "t", &old, &new);
        assert_eq!(diff.replace_keys, vec!["project"]);
    }

    #[test]
    fn rename_cycle_detection() {
        let renames = vec![
            DdlOp::RenameColumn {
                old_name: "a",
                new_name: "b",
            },
            DdlOp::RenameColumn {
                old_name: "b",
                new_name: "a",
            },
        ];
        let broken = break_rename_cycles(&renames);
        assert!(!broken.is_empty(), "should detect cycle");
    }

    // =====================================================================
    // Successive schema update tests
    // =====================================================================

    #[test]
    fn successive_insert_rename_delete() {
        // Step 0 (initial): [id INT64, name STRING, email STRING]
        let step0 = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            field("email", "STRING"),
        ];

        // Step 1: insert age + city
        let mut age = field("age", "INT64");
        age.alter = Some(AlterAction::Insert);
        let mut city = field("city", "STRING");
        city.alter = Some(AlterAction::Insert);
        let step1_news = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            field("email", "STRING"),
            age,
            city,
        ];
        let diff1 = compute_diff("p", "p", "d", "d", "t", "t", &step0, &step1_news);
        assert!(diff1.has_schema_changes);
        let add_count = diff1
            .ops
            .iter()
            .filter(|op| matches!(op, DdlOp::AddColumn { .. }))
            .count();
        assert_eq!(add_count, 2);
        let stmts1 = build_batch_ddl("p", "d", "t", &diff1.ops, &diff1.owned_ops);
        assert_eq!(stmts1.len(), 1); // 1 batched ADD stmt
        assert!(stmts1[0].contains("ADD COLUMN"));

        let step1_clean = clean_fields(&step1_news);
        assert_eq!(step1_clean.len(), 5);

        // Step 2: rename city → city_name
        let mut city_name = field("city_name", "STRING");
        city_name.alter = Some(AlterAction::Rename);
        city_name.alter_from = Some("city");
        let step2_news = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            field("email", "STRING"),
            field("age", "INT64"),
            city_name,
        ];
        let diff2 = compute_diff("p", "p", "d", "d", "t", "t", &step1_clean, &step2_news);
        assert!(diff2.has_schema_changes);
        let rename_count = diff2
            .ops
            .iter()
            .filter(|op| matches!(op, DdlOp::RenameColumn { .. }))
            .count();
        assert_eq!(rename_count, 1);
        let stmts2 = build_batch_ddl("p", "d", "t", &diff2.ops, &diff2.owned_ops);
        assert_eq!(stmts2.len(), 1); // 1 RENAME stmt
        assert!(stmts2[0].contains("RENAME COLUMN"));

        let step2_clean = clean_fields(&step2_news);
        assert_eq!(step2_clean.len(), 5);

        // Step 3: delete email
        let mut del_email = field("email", "STRING");
        del_email.alter = Some(AlterAction::Delete);
        let step3_news = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            del_email,
            field("age", "INT64"),
            field("city_name", "STRING"),
        ];
        let diff3 = compute_diff("p", "p", "d", "d", "t", "t", &step2_clean, &step3_news);
        assert!(diff3.has_schema_changes);
        let drop_count = diff3
            .ops
            .iter()
            .filter(|op| matches!(op, DdlOp::DropColumn { .. }))
            .count();
        assert_eq!(drop_count, 1);
        let stmts3 = build_batch_ddl("p", "d", "t", &diff3.ops, &diff3.owned_ops);
        assert_eq!(stmts3.len(), 1); // 1 DROP stmt
        assert!(stmts3[0].contains("DROP COLUMN"));

        let step3_clean = clean_fields(&step3_news);
        assert_eq!(step3_clean.len(), 4); // id, name, age, city_name
        assert!(step3_clean.iter().all(|f| f.alter.is_none()));
    }

    #[test]
    fn combined_insert_rename_delete_single_diff() {
        let olds = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            field("old_col", "STRING"),
        ];

        // In a single diff: drop old_col, rename name→full_name, add created_at + new_col
        let mut del = field("old_col", "STRING");
        del.alter = Some(AlterAction::Delete);
        let mut ren = field("full_name", "STRING");
        ren.alter = Some(AlterAction::Rename);
        ren.alter_from = Some("name");
        let mut ins1 = field("new_col", "INT64");
        ins1.alter = Some(AlterAction::Insert);
        let mut ins2 = field("created_at", "TIMESTAMP");
        ins2.alter = Some(AlterAction::Insert);

        let news = vec![field("id", "INT64"), ren, del, ins1, ins2];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &olds, &news);
        assert!(diff.has_schema_changes);

        // Verify ordering: drops first, then renames, then adds
        let mut saw_drop = false;
        let mut saw_rename = false;
        let mut saw_add = false;
        for op in &diff.ops {
            match op {
                DdlOp::DropColumn { .. } => {
                    assert!(
                        !saw_rename && !saw_add,
                        "drops should come before renames/adds"
                    );
                    saw_drop = true;
                }
                DdlOp::RenameColumn { .. } => {
                    assert!(
                        saw_drop || !saw_add,
                        "renames should come after drops, before adds"
                    );
                    saw_rename = true;
                }
                DdlOp::AddColumn { .. } => {
                    saw_add = true;
                }
                _ => {}
            }
        }
        assert!(saw_drop && saw_rename && saw_add);

        let stmts = build_batch_ddl("p", "d", "t", &diff.ops, &diff.owned_ops);
        assert_eq!(stmts.len(), 3); // DROP, RENAME, ADD

        let cleaned = clean_fields(&news);
        assert_eq!(cleaned.len(), 4); // id, full_name, new_col, created_at
        let names: Vec<&str> = cleaned.iter().map(|f| f.name).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"full_name"));
        assert!(names.contains(&"new_col"));
        assert!(names.contains(&"created_at"));
    }

    #[test]
    fn successive_no_change_between_updates() {
        let fields = vec![
            field("id", "INT64"),
            field("name", "STRING"),
            field("email", "STRING"),
        ];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &fields, &fields);
        assert!(!diff.has_schema_changes);
        assert!(diff.ops.is_empty());
        assert!(diff.owned_ops.is_empty());
    }

    // =====================================================================
    // Idempotent diff tests (declarative schema)
    // =====================================================================

    #[test]
    fn idempotent_insert_already_exists() {
        // Column was already inserted in a previous apply — no DDL generated.
        let old = vec![field("id", "INT64"), field("age", "INT64")];
        let mut ins = field("age", "INT64");
        ins.alter = Some(AlterAction::Insert);
        let new = vec![field("id", "INT64"), ins];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(!diff.has_schema_changes);
        assert!(diff.ops.is_empty());
    }

    #[test]
    fn idempotent_delete_already_gone() {
        // Column was already deleted — no DDL generated.
        let old = vec![field("id", "INT64")];
        let mut del = field("email", "STRING");
        del.alter = Some(AlterAction::Delete);
        let new = vec![field("id", "INT64"), del];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(!diff.has_schema_changes);
        assert!(diff.ops.is_empty());
    }

    #[test]
    fn idempotent_rename_already_applied() {
        // Column was already renamed — no DDL generated.
        let old = vec![field("id", "INT64"), field("new_name", "STRING")];
        let mut ren = field("new_name", "STRING");
        ren.alter = Some(AlterAction::Rename);
        ren.alter_from = Some("old_name");
        let new = vec![field("id", "INT64"), ren];
        let diff = compute_diff("p", "p", "d", "d", "t", "t", &old, &new);
        assert!(!diff.has_schema_changes);
        assert!(diff.ops.is_empty());
    }
}
