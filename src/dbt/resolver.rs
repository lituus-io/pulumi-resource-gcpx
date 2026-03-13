use std::collections::BTreeMap;

use crate::dbt::macros::{expand_macros, MacroDef};
use crate::dbt::options::TableOptions;
use crate::dbt::scanner::{DbtScanner, DbtSegment};
use crate::dbt::types::{ModelRefData, PartitionConfig, ResolvedSql, SourceDef};
use crate::sanitize::{bq_col_ref, bq_table_ref};

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("macro expansion failed: {0}")]
    MacroExpansion(#[from] crate::dbt::macros::MacroError),
    #[error("unknown ref '{0}'")]
    UnknownRef(String),
    #[error("unknown source '{src}.{table}'")]
    UnknownSource { src: String, table: String },
}

/// Resolve a dbt model's SQL: expand macros, resolve refs and sources, flatten CTEs.
pub fn resolve(
    sql: &str,
    project: &str,
    _dataset: &str,
    sources: &BTreeMap<String, SourceDef>,
    model_refs: &BTreeMap<String, ModelRefData>,
    macros: &BTreeMap<String, MacroDef>,
) -> Result<ResolvedSql, ResolveError> {
    // Phase 1: Macro expansion.
    let expanded = expand_macros(sql, macros)?;

    // Phase 2: Scan for refs/sources and build resolved SQL.
    let mut ctes: Vec<(String, String)> = Vec::new();
    let mut body = String::with_capacity(expanded.len());
    let mut seen_ctes = BTreeMap::new();

    for segment in DbtScanner::new(&expanded) {
        match segment {
            DbtSegment::Config { .. } => {
                // Config is consumed by validation, not included in output SQL.
            }
            DbtSegment::Sql(s) => {
                body.push_str(s);
            }
            DbtSegment::Ref { model } => {
                if let Some(ref_data) = model_refs.get(model) {
                    if ref_data.materialization == "ephemeral" {
                        // Inline ephemeral: prepend CTEs, reference as CTE name.
                        let cte_name = format!("__dbt__cte__{}", model);
                        if !seen_ctes.contains_key(&cte_name) {
                            // Add upstream CTEs from the ephemeral model.
                            if !ref_data.resolved_ctes_json.is_empty()
                                && ref_data.resolved_ctes_json != "[]"
                            {
                                if let Ok(upstream) = serde_json::from_str::<Vec<(String, String)>>(
                                    &ref_data.resolved_ctes_json,
                                ) {
                                    for (name, sql) in upstream {
                                        if !seen_ctes.contains_key(&name) {
                                            seen_ctes.insert(name.clone(), ctes.len());
                                            ctes.push((name, sql));
                                        }
                                    }
                                }
                            }
                            // Add this ephemeral model as a CTE.
                            seen_ctes.insert(cte_name.clone(), ctes.len());
                            ctes.push((cte_name.clone(), ref_data.resolved_body.clone()));
                        }
                        body.push_str(&cte_name);
                    } else {
                        // Non-ephemeral: use fully-qualified table reference.
                        body.push_str(&ref_data.table_ref);
                    }
                } else {
                    return Err(ResolveError::UnknownRef(model.to_owned()));
                }
            }
            DbtSegment::Source { source, table } => {
                if let Some(src_def) = sources.get(source) {
                    body.push_str(&bq_table_ref(project, &src_def.dataset, table));
                } else {
                    return Err(ResolveError::UnknownSource {
                        src: source.to_owned(),
                        table: table.to_owned(),
                    });
                }
            }
            DbtSegment::Call { name, raw_args } => {
                // After macro expansion, any remaining calls are unknown macros.
                body.push_str("{{ ");
                body.push_str(name);
                body.push('(');
                body.push_str(raw_args);
                body.push_str(") }}");
            }
        }
    }

    Ok(ResolvedSql {
        ctes,
        body: body.trim().to_owned(),
    })
}

// ═══════════════════════════════════════════════════════════════════════
// DDL Generation
// ═══════════════════════════════════════════════════════════════════════

/// Generate DDL from resolved SQL based on materialization.
/// Supports PARTITION BY, CLUSTER BY, and OPTIONS for table/incremental/view/materialized_view.
#[allow(clippy::too_many_arguments)]
pub fn generate_ddl(
    project: &str,
    dataset: &str,
    name: &str,
    materialization: &str,
    resolved: &ResolvedSql,
    partition_by: Option<&PartitionConfig>,
    cluster_by: Option<&[String]>,
    options: Option<&TableOptions<'_>>,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();

    let options_clause = options
        .filter(|o| !o.is_empty())
        .map(|o| o.to_ddl())
        .unwrap_or_default();

    match materialization {
        "table" | "incremental" => {
            let mut ddl = format!("CREATE OR REPLACE TABLE {}", table_ref);
            if let Some(part) = partition_by {
                ddl.push_str(&format!("\nPARTITION BY {}", part.to_ddl()));
            }
            if let Some(cols) = cluster_by {
                if !cols.is_empty() {
                    ddl.push_str(&format!("\nCLUSTER BY {}", cols.join(", ")));
                }
            }
            if !options_clause.is_empty() {
                ddl.push('\n');
                ddl.push_str(&options_clause);
            }
            ddl.push_str(&format!(" AS {}", full_sql));
            ddl
        }
        "view" => {
            let mut ddl = format!("CREATE OR REPLACE VIEW {}", table_ref);
            if !options_clause.is_empty() {
                ddl.push('\n');
                ddl.push_str(&options_clause);
            }
            ddl.push_str(&format!(" AS {}", full_sql));
            ddl
        }
        "materialized_view" => {
            let mut ddl = format!("CREATE MATERIALIZED VIEW {}", table_ref);
            if !options_clause.is_empty() {
                ddl.push('\n');
                ddl.push_str(&options_clause);
            }
            ddl.push_str(&format!(" AS {}", full_sql));
            ddl
        }
        "ephemeral" => String::new(),
        _ => String::new(),
    }
}

/// Generate MERGE DDL with proper UPDATE SET for incremental models.
/// Uses runtime column names from get_table_schema().
pub fn generate_merge_ddl(
    project: &str,
    dataset: &str,
    name: &str,
    unique_keys: &[String],
    columns: &[String],
    resolved: &ResolvedSql,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();

    let on_clause = unique_keys
        .iter()
        .map(|k| format!("T.{} = S.{}", bq_col_ref(k), bq_col_ref(k)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let update_set = columns
        .iter()
        .filter(|c| !unique_keys.iter().any(|k| k.eq_ignore_ascii_case(c)))
        .map(|c| format!("T.{} = S.{}", bq_col_ref(c), bq_col_ref(c)))
        .collect::<Vec<_>>()
        .join(", ");

    if update_set.is_empty() {
        // All columns are key columns — no UPDATE SET needed.
        format!(
            "MERGE {} T USING ({}) S ON {} WHEN NOT MATCHED THEN INSERT ROW",
            table_ref, full_sql, on_clause
        )
    } else {
        format!(
            "MERGE {} T USING ({}) S ON {} \
             WHEN MATCHED THEN UPDATE SET {} \
             WHEN NOT MATCHED THEN INSERT ROW",
            table_ref, full_sql, on_clause, update_set
        )
    }
}

/// Generate INSERT-only MERGE DDL (placeholder when table doesn't exist in preview).
pub fn generate_merge_ddl_placeholder(
    project: &str,
    dataset: &str,
    name: &str,
    unique_keys: &[String],
    resolved: &ResolvedSql,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();

    let on_clause = unique_keys
        .iter()
        .map(|k| format!("T.{} = S.{}", bq_col_ref(k), bq_col_ref(k)))
        .collect::<Vec<_>>()
        .join(" AND ");

    format!(
        "MERGE {} T USING ({}) S ON {} WHEN NOT MATCHED THEN INSERT ROW",
        table_ref, full_sql, on_clause
    )
}

/// Generate DELETE+INSERT DDL for incremental models (BigQuery scripting).
pub fn generate_delete_insert_ddl(
    project: &str,
    dataset: &str,
    name: &str,
    unique_keys: &[String],
    resolved: &ResolvedSql,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();

    let key_cols = unique_keys
        .iter()
        .map(|k| bq_col_ref(k))
        .collect::<Vec<_>>();

    let delete_where = if key_cols.len() == 1 {
        format!("{} IN (SELECT {} FROM __src)", key_cols[0], key_cols[0])
    } else {
        let tuple = key_cols.join(", ");
        format!("({}) IN (SELECT {} FROM __src)", tuple, tuple)
    };

    format!(
        "BEGIN\n  \
         CREATE TEMP TABLE __src AS ({});\n  \
         DELETE FROM {} WHERE {};\n  \
         INSERT INTO {} SELECT * FROM __src;\n  \
         DROP TABLE __src;\n\
         END;",
        full_sql, table_ref, delete_where, table_ref
    )
}

/// Generate APPEND DDL for incremental models (insert-only, no dedup).
pub fn generate_append_ddl(
    project: &str,
    dataset: &str,
    name: &str,
    resolved: &ResolvedSql,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();
    format!("INSERT INTO {} {}", table_ref, full_sql)
}

/// Generate MERGE-based full replacement DDL (for materialized='table' with tableId).
/// Preserves table metadata, partitioning, clustering, column descriptions.
pub fn generate_merge_replace_ddl(
    project: &str,
    dataset: &str,
    name: &str,
    resolved: &ResolvedSql,
) -> String {
    let table_ref = bq_table_ref(project, dataset, name);
    let full_sql = resolved.to_sql();
    format!(
        "MERGE {} T USING ({}) S ON FALSE \
         WHEN NOT MATCHED BY TARGET THEN INSERT ROW \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
        table_ref, full_sql
    )
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sources() -> BTreeMap<String, SourceDef> {
        let mut sources = BTreeMap::new();
        sources.insert(
            "raw".to_owned(),
            SourceDef {
                dataset: "raw_data".to_owned(),
                tables: vec!["customers".to_owned()],
            },
        );
        sources
    }

    #[test]
    fn resolve_source() {
        let sql = "SELECT * FROM {{ source('raw', 'customers') }}";
        let sources = make_sources();
        let refs = BTreeMap::new();
        let macros = BTreeMap::new();
        let result = resolve(sql, "proj", "ds", &sources, &refs, &macros).unwrap();
        assert!(result.body.contains("`proj.raw_data.customers`"));
    }

    #[test]
    fn resolve_non_ephemeral_ref() {
        let sql = "SELECT * FROM {{ ref('stg') }}";
        let sources = BTreeMap::new();
        let mut refs = BTreeMap::new();
        refs.insert(
            "stg".to_owned(),
            ModelRefData {
                materialization: "view".to_owned(),
                resolved_ctes_json: String::new(),
                resolved_body: String::new(),
                table_ref: "`proj.ds.stg`".to_owned(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
        let macros = BTreeMap::new();
        let result = resolve(sql, "proj", "ds", &sources, &refs, &macros).unwrap();
        assert!(result.body.contains("`proj.ds.stg`"));
        assert!(result.ctes.is_empty());
    }

    #[test]
    fn resolve_ephemeral_ref() {
        let sql = "SELECT * FROM {{ ref('eph') }}";
        let sources = BTreeMap::new();
        let mut refs = BTreeMap::new();
        refs.insert(
            "eph".to_owned(),
            ModelRefData {
                materialization: "ephemeral".to_owned(),
                resolved_ctes_json: "[]".to_owned(),
                resolved_body: "SELECT 1".to_owned(),
                table_ref: String::new(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
        let macros = BTreeMap::new();
        let result = resolve(sql, "proj", "ds", &sources, &refs, &macros).unwrap();
        assert!(result.body.contains("__dbt__cte__eph"));
        assert_eq!(result.ctes.len(), 1);
        assert_eq!(result.ctes[0].0, "__dbt__cte__eph");
        assert_eq!(result.ctes[0].1, "SELECT 1");
    }

    #[test]
    fn resolve_unknown_ref() {
        let sql = "SELECT * FROM {{ ref('missing') }}";
        let result = resolve(
            sql,
            "proj",
            "ds",
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn resolve_with_macros() {
        let sql = "SELECT {{ sk('id') }} FROM {{ source('raw', 'customers') }}";
        let sources = make_sources();
        let refs = BTreeMap::new();
        let mut macros = BTreeMap::new();
        macros.insert(
            "sk".to_owned(),
            MacroDef {
                args: vec!["cols".to_owned()],
                sql: "MD5({{ cols }})".to_owned(),
            },
        );
        let result = resolve(sql, "proj", "ds", &sources, &refs, &macros).unwrap();
        assert!(result.body.contains("MD5(id)"));
        assert!(result.body.contains("`proj.raw_data.customers`"));
    }

    // ── DDL generation ─────────────────────────────────────────────

    #[test]
    fn generate_ddl_table() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let ddl = generate_ddl("p", "d", "t", "table", &resolved, None, None, None);
        assert_eq!(ddl, "CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1");
    }

    #[test]
    fn generate_ddl_view() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let ddl = generate_ddl("p", "d", "v", "view", &resolved, None, None, None);
        assert_eq!(ddl, "CREATE OR REPLACE VIEW `p.d.v` AS SELECT 1");
    }

    #[test]
    fn generate_ddl_ephemeral_empty() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let ddl = generate_ddl("p", "d", "e", "ephemeral", &resolved, None, None, None);
        assert!(ddl.is_empty());
    }

    #[test]
    fn generate_ddl_with_partition_by() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let part = PartitionConfig {
            field: "order_date".to_owned(),
            data_type: "date".to_owned(),
            granularity: None,
        };
        let ddl = generate_ddl("p", "d", "t", "table", &resolved, Some(&part), None, None);
        assert!(ddl.contains("PARTITION BY order_date"));
    }

    #[test]
    fn generate_ddl_with_cluster_by() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let cols = vec!["customer_id".to_owned(), "region".to_owned()];
        let ddl = generate_ddl("p", "d", "t", "table", &resolved, None, Some(&cols), None);
        assert!(ddl.contains("CLUSTER BY customer_id, region"));
    }

    #[test]
    fn generate_ddl_partition_and_cluster() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let part = PartitionConfig {
            field: "ts".to_owned(),
            data_type: "timestamp".to_owned(),
            granularity: Some("day".to_owned()),
        };
        let cols = vec!["id".to_owned()];
        let ddl = generate_ddl(
            "p",
            "d",
            "t",
            "incremental",
            &resolved,
            Some(&part),
            Some(&cols),
            None,
        );
        assert!(ddl.contains("PARTITION BY TIMESTAMP_TRUNC(ts, DAY)"));
        assert!(ddl.contains("CLUSTER BY id"));
        assert!(ddl.contains("CREATE OR REPLACE TABLE"));
    }

    // ── MERGE DDL ──────────────────────────────────────────────────

    #[test]
    fn test_generate_merge_ddl_with_columns() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT * FROM src".to_owned(),
        };
        let keys = vec!["id".to_owned()];
        let columns = vec!["id".to_owned(), "name".to_owned(), "email".to_owned()];
        let ddl = generate_merge_ddl("p", "d", "t", &keys, &columns, &resolved);
        assert!(ddl.contains("MERGE `p.d.t` T USING"));
        assert!(ddl.contains("ON T.`id` = S.`id`"));
        assert!(ddl.contains("WHEN MATCHED THEN UPDATE SET"));
        assert!(ddl.contains("T.`name` = S.`name`"));
        assert!(ddl.contains("T.`email` = S.`email`"));
        assert!(!ddl.contains("T.`id` = S.`id`,"));
        assert!(ddl.contains("WHEN NOT MATCHED THEN INSERT ROW"));
    }

    #[test]
    fn test_generate_merge_ddl_excludes_key() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let keys = vec!["pk".to_owned()];
        let columns = vec!["pk".to_owned(), "val".to_owned()];
        let ddl = generate_merge_ddl("p", "d", "t", &keys, &columns, &resolved);
        let update_part = ddl.split("UPDATE SET ").nth(1).unwrap();
        assert!(!update_part.starts_with("T.`pk`"));
        assert!(update_part.contains("T.`val` = S.`val`"));
    }

    #[test]
    fn test_composite_unique_key_merge() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let keys = vec!["order_id".to_owned(), "line_item_id".to_owned()];
        let columns = vec![
            "order_id".to_owned(),
            "line_item_id".to_owned(),
            "amount".to_owned(),
        ];
        let ddl = generate_merge_ddl("p", "d", "t", &keys, &columns, &resolved);
        assert!(ddl.contains("T.`order_id` = S.`order_id` AND T.`line_item_id` = S.`line_item_id`"));
        assert!(ddl.contains("T.`amount` = S.`amount`"));
    }

    // ── DELETE+INSERT DDL ──────────────────────────────────────────

    #[test]
    fn test_generate_delete_insert_ddl() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT * FROM src".to_owned(),
        };
        let keys = vec!["id".to_owned()];
        let ddl = generate_delete_insert_ddl("p", "d", "t", &keys, &resolved);
        assert!(ddl.contains("BEGIN"));
        assert!(ddl.contains("CREATE TEMP TABLE __src"));
        assert!(ddl.contains("DELETE FROM `p.d.t`"));
        assert!(ddl.contains("INSERT INTO `p.d.t` SELECT * FROM __src"));
        assert!(ddl.contains("DROP TABLE __src"));
        assert!(ddl.contains("END;"));
    }

    #[test]
    fn test_generate_delete_insert_composite_key() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let keys = vec!["a".to_owned(), "b".to_owned()];
        let ddl = generate_delete_insert_ddl("p", "d", "t", &keys, &resolved);
        assert!(ddl.contains("(`a`, `b`) IN (SELECT `a`, `b` FROM __src)"));
    }

    // ── APPEND DDL ─────────────────────────────────────────────────

    #[test]
    fn test_generate_append_ddl() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT * FROM src".to_owned(),
        };
        let ddl = generate_append_ddl("p", "d", "t", &resolved);
        assert_eq!(ddl, "INSERT INTO `p.d.t` SELECT * FROM src");
    }

    // ── MERGE REPLACE DDL ──────────────────────────────────────────

    #[test]
    fn test_generate_merge_replace_ddl() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1 AS id".to_owned(),
        };
        let ddl = generate_merge_replace_ddl("p", "d", "t", &resolved);
        assert!(ddl.contains("MERGE `p.d.t` T USING"));
        assert!(ddl.contains("ON FALSE"));
        assert!(ddl.contains("WHEN NOT MATCHED BY TARGET THEN INSERT ROW"));
        assert!(ddl.contains("WHEN NOT MATCHED BY SOURCE THEN DELETE"));
    }

    // ── Existing tests preserved ───────────────────────────────────

    fn make_ephemeral_ref(_name: &str, body: &str, upstream_ctes: &str) -> ModelRefData {
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: upstream_ctes.to_owned(),
            resolved_body: body.to_owned(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        }
    }

    fn make_table_ref(_name: &str, table_ref: &str) -> ModelRefData {
        ModelRefData {
            materialization: "view".to_owned(),
            resolved_ctes_json: String::new(),
            resolved_body: String::new(),
            table_ref: table_ref.to_owned(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        }
    }

    #[test]
    fn resolve_transitive_ephemeral_chain() {
        let sql = "SELECT * FROM {{ ref('b') }}";
        let mut refs = BTreeMap::new();
        let upstream_ctes =
            serde_json::to_string(&vec![("__dbt__cte__a".to_owned(), "SELECT 1".to_owned())])
                .unwrap();
        refs.insert(
            "b".to_owned(),
            make_ephemeral_ref("b", "SELECT * FROM __dbt__cte__a", &upstream_ctes),
        );
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &refs, &BTreeMap::new()).unwrap();
        assert_eq!(result.ctes.len(), 2);
        assert_eq!(result.ctes[0].0, "__dbt__cte__a");
        assert_eq!(result.ctes[1].0, "__dbt__cte__b");
        assert!(result.body.contains("__dbt__cte__b"));
    }

    #[test]
    fn resolve_multiple_refs_same_model() {
        let sql = "SELECT * FROM {{ ref('x') }} UNION ALL SELECT * FROM {{ ref('x') }}";
        let mut refs = BTreeMap::new();
        refs.insert("x".to_owned(), make_ephemeral_ref("x", "SELECT 1", "[]"));
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &refs, &BTreeMap::new()).unwrap();
        assert_eq!(result.ctes.len(), 1);
        assert_eq!(result.ctes[0].0, "__dbt__cte__x");
        let count = result.body.matches("__dbt__cte__x").count();
        assert_eq!(count, 2);
    }

    #[test]
    fn resolve_mixed_refs() {
        let sql = "SELECT * FROM {{ ref('eph') }} JOIN {{ ref('tbl') }}";
        let mut refs = BTreeMap::new();
        refs.insert(
            "eph".to_owned(),
            make_ephemeral_ref("eph", "SELECT 1", "[]"),
        );
        refs.insert("tbl".to_owned(), make_table_ref("tbl", "`p.d.tbl`"));
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &refs, &BTreeMap::new()).unwrap();
        assert_eq!(result.ctes.len(), 1);
        assert!(result.body.contains("__dbt__cte__eph"));
        assert!(result.body.contains("`p.d.tbl`"));
    }

    #[test]
    fn resolve_source_and_ref_combined() {
        let sql = "SELECT * FROM {{ source('raw', 'customers') }} JOIN {{ ref('stg') }}";
        let sources = make_sources();
        let mut refs = BTreeMap::new();
        refs.insert("stg".to_owned(), make_table_ref("stg", "`p.d.stg`"));
        let result = resolve(sql, "p", "d", &sources, &refs, &BTreeMap::new()).unwrap();
        assert!(result.body.contains("`p.raw_data.customers`"));
        assert!(result.body.contains("`p.d.stg`"));
    }

    #[test]
    fn resolve_multiple_sources() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "raw".to_owned(),
            SourceDef {
                dataset: "raw_data".to_owned(),
                tables: vec!["customers".to_owned(), "orders".to_owned()],
            },
        );
        sources.insert(
            "staging".to_owned(),
            SourceDef {
                dataset: "stg_data".to_owned(),
                tables: vec!["products".to_owned()],
            },
        );
        let sql = "SELECT * FROM {{ source('raw', 'customers') }} JOIN {{ source('staging', 'products') }}";
        let result = resolve(sql, "p", "d", &sources, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert!(result.body.contains("`p.raw_data.customers`"));
        assert!(result.body.contains("`p.stg_data.products`"));
    }

    #[test]
    fn resolve_multiple_macros() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "upper".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "UPPER({{ x }})".to_owned(),
            },
        );
        macros.insert(
            "lower".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "LOWER({{ x }})".to_owned(),
            },
        );
        let sql = "SELECT {{ upper('name') }}, {{ lower('email') }}";
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &BTreeMap::new(), &macros).unwrap();
        assert!(result.body.contains("UPPER(name)"));
        assert!(result.body.contains("LOWER(email)"));
    }

    #[test]
    fn resolve_empty_sql_after_config() {
        let sql = "{{ config(materialized='table') }}";
        let result = resolve(
            sql,
            "p",
            "d",
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(result.body.is_empty() || result.body.trim().is_empty());
    }

    #[test]
    fn generate_ddl_materialized_view() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let ddl = generate_ddl(
            "p",
            "d",
            "mv",
            "materialized_view",
            &resolved,
            None,
            None,
            None,
        );
        assert_eq!(ddl, "CREATE MATERIALIZED VIEW `p.d.mv` AS SELECT 1");
    }

    #[test]
    fn generate_ddl_with_ctes() {
        let resolved = ResolvedSql {
            ctes: vec![("__dbt__cte__x".to_owned(), "SELECT 1".to_owned())],
            body: "SELECT * FROM __dbt__cte__x".to_owned(),
        };
        let ddl = generate_ddl("p", "d", "t", "table", &resolved, None, None, None);
        assert!(ddl.contains("WITH __dbt__cte__x AS (SELECT 1)"));
        assert!(ddl.contains("SELECT * FROM __dbt__cte__x"));
        assert!(ddl.starts_with("CREATE OR REPLACE TABLE"));
    }

    #[test]
    fn resolve_deep_ephemeral_chain_5_levels() {
        let mut refs = BTreeMap::new();
        let ctes_abcd = serde_json::to_string(&vec![
            ("__dbt__cte__a".to_owned(), "SELECT 1".to_owned()),
            (
                "__dbt__cte__b".to_owned(),
                "SELECT * FROM __dbt__cte__a".to_owned(),
            ),
            (
                "__dbt__cte__c".to_owned(),
                "SELECT * FROM __dbt__cte__b".to_owned(),
            ),
            (
                "__dbt__cte__d".to_owned(),
                "SELECT * FROM __dbt__cte__c".to_owned(),
            ),
        ])
        .unwrap();
        refs.insert(
            "e".to_owned(),
            make_ephemeral_ref("e", "SELECT * FROM __dbt__cte__d", &ctes_abcd),
        );
        let sql = "SELECT * FROM {{ ref('e') }}";
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &refs, &BTreeMap::new()).unwrap();
        assert_eq!(result.ctes.len(), 5);
        assert_eq!(result.ctes[0].0, "__dbt__cte__a");
        assert_eq!(result.ctes[4].0, "__dbt__cte__e");
    }

    #[test]
    fn resolve_diamond_dependency() {
        let mut refs = BTreeMap::new();
        let a_ctes =
            serde_json::to_string(&vec![("__dbt__cte__a".to_owned(), "SELECT 1".to_owned())])
                .unwrap();
        refs.insert(
            "b".to_owned(),
            make_ephemeral_ref("b", "SELECT * FROM __dbt__cte__a WHERE x=1", &a_ctes),
        );
        refs.insert(
            "c".to_owned(),
            make_ephemeral_ref("c", "SELECT * FROM __dbt__cte__a WHERE x=2", &a_ctes),
        );
        let sql = "SELECT * FROM {{ ref('b') }} UNION ALL SELECT * FROM {{ ref('c') }}";
        let result = resolve(sql, "p", "d", &BTreeMap::new(), &refs, &BTreeMap::new()).unwrap();
        let a_count = result
            .ctes
            .iter()
            .filter(|(n, _)| n == "__dbt__cte__a")
            .count();
        assert_eq!(a_count, 1);
        assert!(result.ctes.iter().any(|(n, _)| n == "__dbt__cte__b"));
        assert!(result.ctes.iter().any(|(n, _)| n == "__dbt__cte__c"));
    }

    #[test]
    fn generate_ddl_incremental_creates_table() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT * FROM source_table".to_owned(),
        };
        let ddl = generate_ddl("p", "d", "t", "incremental", &resolved, None, None, None);
        assert_eq!(
            ddl,
            "CREATE OR REPLACE TABLE `p.d.t` AS SELECT * FROM source_table"
        );
    }

    #[test]
    fn generate_merge_ddl_with_ctes() {
        let resolved = ResolvedSql {
            ctes: vec![("__dbt__cte__x".to_owned(), "SELECT 1".to_owned())],
            body: "SELECT * FROM __dbt__cte__x".to_owned(),
        };
        let keys = vec!["order_id".to_owned()];
        let columns = vec!["order_id".to_owned(), "amount".to_owned()];
        let ddl = generate_merge_ddl("p", "d", "t", &keys, &columns, &resolved);
        assert!(ddl.contains("WITH __dbt__cte__x AS (SELECT 1)"));
        assert!(ddl.contains("ON T.`order_id` = S.`order_id`"));
    }

    // ── OPTIONS tests ───────────────────────────────────────────────

    #[test]
    fn generate_ddl_table_with_options() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let opts = TableOptions {
            require_partition_filter: Some(true),
            friendly_name: Some("My Table"),
            ..Default::default()
        };
        let part = PartitionConfig {
            field: "ts".to_owned(),
            data_type: "date".to_owned(),
            granularity: None,
        };
        let ddl = generate_ddl(
            "p",
            "d",
            "t",
            "table",
            &resolved,
            Some(&part),
            None,
            Some(&opts),
        );
        assert!(ddl.contains("PARTITION BY ts"));
        assert!(ddl.contains("OPTIONS("));
        assert!(ddl.contains("require_partition_filter = true"));
        assert!(ddl.contains("friendly_name = \"My Table\""));
        assert!(ddl.contains("AS SELECT 1"));
    }

    #[test]
    fn generate_ddl_view_with_options() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let opts = TableOptions {
            description: Some("test view"),
            ..Default::default()
        };
        let ddl = generate_ddl("p", "d", "v", "view", &resolved, None, None, Some(&opts));
        assert!(ddl.contains("CREATE OR REPLACE VIEW"));
        assert!(ddl.contains("OPTIONS(description = \"test view\")"));
    }

    #[test]
    fn generate_ddl_mv_with_options() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let opts = TableOptions {
            enable_refresh: Some(true),
            refresh_interval_minutes: Some(30),
            ..Default::default()
        };
        let ddl = generate_ddl(
            "p",
            "d",
            "mv",
            "materialized_view",
            &resolved,
            None,
            None,
            Some(&opts),
        );
        assert!(ddl.contains("CREATE MATERIALIZED VIEW"));
        assert!(ddl.contains("enable_refresh = true"));
        assert!(ddl.contains("refresh_interval_minutes = 30"));
    }

    #[test]
    fn generate_ddl_empty_options_no_clause() {
        let resolved = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        let opts = TableOptions::default();
        let ddl = generate_ddl("p", "d", "t", "table", &resolved, None, None, Some(&opts));
        assert!(!ddl.contains("OPTIONS"));
        assert_eq!(ddl, "CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn ddl_generators_never_panic(
            project in "[a-z]{1,10}",
            dataset in "[a-z]{1,10}",
            name in "[a-z]{1,10}",
            body in "[ -~]{0,200}",
            mat in prop_oneof!["table", "view", "incremental", "ephemeral", "materialized_view"],
        ) {
            let resolved = ResolvedSql {
                ctes: vec![],
                body: body.clone(),
            };
            let _ = std::hint::black_box(generate_ddl(&project, &dataset, &name, &mat, &resolved, None, None, None));
            let _ = std::hint::black_box(generate_append_ddl(&project, &dataset, &name, &resolved));
            let _ = std::hint::black_box(generate_merge_replace_ddl(&project, &dataset, &name, &resolved));
        }

        #[test]
        fn merge_ddl_never_panics(
            project in "[a-z]{1,10}",
            dataset in "[a-z]{1,10}",
            name in "[a-z]{1,10}",
            body in "[ -~]{0,100}",
            key in "[a-z]{1,10}",
            col1 in "[a-z]{1,10}",
            col2 in "[a-z]{1,10}",
        ) {
            let resolved = ResolvedSql {
                ctes: vec![],
                body,
            };
            let keys = vec![key.clone()];
            let columns = vec![key, col1, col2];
            let _ = std::hint::black_box(generate_merge_ddl(&project, &dataset, &name, &keys, &columns, &resolved));
            let _ = std::hint::black_box(generate_merge_ddl_placeholder(&project, &dataset, &name, &keys, &resolved));
            let _ = std::hint::black_box(generate_delete_insert_ddl(&project, &dataset, &name, &keys, &resolved));
        }
    }
}
