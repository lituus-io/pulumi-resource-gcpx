use std::fmt::Write;

use crate::sanitize::{bq_table_ref, escape_bq_ident};
use crate::schema::types::{DdlOp, OwnedDdlOp};

/// Build batched DDL statements from DdlOps and any cycle-breaking OwnedDdlOps.
///
/// Groups same-type ops into single ALTER TABLE statements where possible.
/// Uses write!() to a pre-sized String to minimize allocations.
pub fn build_batch_ddl(
    project: &str,
    dataset: &str,
    table_id: &str,
    ops: &[DdlOp<'_>],
    owned_ops: &[OwnedDdlOp],
) -> Vec<String> {
    let table_ref = bq_table_ref(project, dataset, table_id);
    let mut stmts = Vec::new();

    // 1. Batch all DROPs.
    let drops: Vec<&str> = ops
        .iter()
        .filter_map(|op| match op {
            DdlOp::DropColumn { name } => Some(*name),
            _ => None,
        })
        .collect();
    if !drops.is_empty() {
        let mut stmt = String::with_capacity(64 + drops.len() * 40);
        write!(stmt, "ALTER TABLE {}", table_ref).unwrap();
        for (i, name) in drops.iter().enumerate() {
            if i > 0 {
                stmt.push(',');
            }
            write!(stmt, " DROP COLUMN IF EXISTS `{}`", escape_bq_ident(name)).unwrap();
        }
        stmts.push(stmt);
    }

    // 2. Cycle-breaking renames first (must happen before regular renames).
    for op in owned_ops {
        let OwnedDdlOp::RenameColumn { old_name, new_name } = op;
        let mut stmt = String::with_capacity(64);
        write!(
            stmt,
            "ALTER TABLE {} RENAME COLUMN `{}` TO `{}`",
            table_ref,
            escape_bq_ident(old_name),
            escape_bq_ident(new_name),
        )
        .unwrap();
        stmts.push(stmt);
    }

    // 3. Regular renames (each as individual statement).
    for op in ops {
        if let DdlOp::RenameColumn { old_name, new_name } = op {
            let mut stmt = String::with_capacity(64);
            write!(
                stmt,
                "ALTER TABLE {} RENAME COLUMN `{}` TO `{}`",
                table_ref, old_name, new_name,
            )
            .unwrap();
            stmts.push(stmt);
        }
    }

    // 4. Batch all ADDs.
    let adds: Vec<_> = ops
        .iter()
        .filter_map(|op| match op {
            DdlOp::AddColumn {
                name,
                field_type,
                mode: _,
                description,
                default_value_expression,
                rounding_mode,
            } => Some((
                *name,
                *field_type,
                *description,
                *default_value_expression,
                *rounding_mode,
            )),
            _ => None,
        })
        .collect();
    if !adds.is_empty() {
        let mut stmt = String::with_capacity(64 + adds.len() * 60);
        write!(stmt, "ALTER TABLE {}", table_ref).unwrap();
        for (i, (name, field_type, description, dve, rm)) in adds.iter().enumerate() {
            if i > 0 {
                stmt.push(',');
            }
            write!(
                stmt,
                " ADD COLUMN IF NOT EXISTS `{}` {}",
                escape_bq_ident(name),
                field_type
            )
            .unwrap();
            if let Some(expr) = dve {
                write!(stmt, " DEFAULT {}", expr).unwrap();
            }
            let mut opts = Vec::new();
            if !description.is_empty() {
                let escaped = description.replace('\'', "\\'");
                opts.push(format!("description='{}'", escaped));
            }
            if let Some(mode) = rm {
                opts.push(format!("rounding_mode='{}'", mode));
            }
            if !opts.is_empty() {
                write!(stmt, " OPTIONS({})", opts.join(",")).unwrap();
            }
        }
        stmts.push(stmt);
    }

    // 5. SET OPTIONS for description changes (individual statements).
    for op in ops {
        if let DdlOp::SetDescription { name, description } = op {
            let escaped = description.replace('\'', "\\'");
            let mut stmt = String::with_capacity(128);
            write!(
                stmt,
                "ALTER TABLE {} ALTER COLUMN `{}` SET OPTIONS(description='{}')",
                table_ref,
                escape_bq_ident(name),
                escaped,
            )
            .unwrap();
            stmts.push(stmt);
        }
    }

    stmts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_drops() {
        let ops = vec![
            DdlOp::DropColumn { name: "col_a" },
            DdlOp::DropColumn { name: "col_b" },
        ];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP COLUMN IF EXISTS `col_a`"));
        assert!(stmts[0].contains("DROP COLUMN IF EXISTS `col_b`"));
    }

    #[test]
    fn batch_adds() {
        let ops = vec![
            DdlOp::AddColumn {
                name: "new_a",
                field_type: "STRING",
                mode: "NULLABLE",
                description: "A new column",
                default_value_expression: None,
                rounding_mode: None,
            },
            DdlOp::AddColumn {
                name: "new_b",
                field_type: "INT64",
                mode: "NULLABLE",
                description: "",
                default_value_expression: None,
                rounding_mode: None,
            },
        ];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD COLUMN IF NOT EXISTS `new_a` STRING"));
        assert!(stmts[0].contains("ADD COLUMN IF NOT EXISTS `new_b` INT64"));
    }

    #[test]
    fn individual_renames() {
        let ops = vec![
            DdlOp::RenameColumn {
                old_name: "old_a",
                new_name: "new_a",
            },
            DdlOp::RenameColumn {
                old_name: "old_b",
                new_name: "new_b",
            },
        ];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("RENAME COLUMN `old_a` TO `new_a`"));
        assert!(stmts[1].contains("RENAME COLUMN `old_b` TO `new_b`"));
    }

    #[test]
    fn set_description() {
        let ops = vec![DdlOp::SetDescription {
            name: "col_a",
            description: "updated desc",
        }];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SET OPTIONS(description='updated desc')"));
    }

    #[test]
    fn mixed_operations_ordered() {
        let ops = vec![
            DdlOp::DropColumn { name: "del_col" },
            DdlOp::RenameColumn {
                old_name: "old",
                new_name: "new",
            },
            DdlOp::AddColumn {
                name: "add_col",
                field_type: "BOOL",
                mode: "NULLABLE",
                description: "",
                default_value_expression: None,
                rounding_mode: None,
            },
            DdlOp::SetDescription {
                name: "desc_col",
                description: "new desc",
            },
        ];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert_eq!(stmts.len(), 4);
        assert!(stmts[0].contains("DROP COLUMN"));
        assert!(stmts[1].contains("RENAME COLUMN"));
        assert!(stmts[2].contains("ADD COLUMN"));
        assert!(stmts[3].contains("SET OPTIONS"));
    }

    #[test]
    fn description_escaping() {
        let ops = vec![DdlOp::SetDescription {
            name: "col",
            description: "it's a test",
        }];
        let stmts = build_batch_ddl("proj", "ds", "tbl", &ops, &[]);
        assert!(stmts[0].contains("it\\'s a test"));
    }
}
