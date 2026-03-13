use crate::sanitize::{bq_table_ref, escape_bq_ident};
use crate::snapshot::types::SnapshotInputs;

/// Generate CREATE TABLE DDL for the snapshot table with SCD Type 2 columns.
/// When `auto_optimize` is true (the default), adds PARTITION BY DATE(dbt_valid_from)
/// and CLUSTER BY `unique_key`.
pub fn generate_snapshot_create_ddl(inputs: &SnapshotInputs<'_>) -> String {
    let table_ref = bq_table_ref(inputs.project, inputs.dataset, inputs.name);
    let auto_optimize = inputs.auto_optimize.unwrap_or(true);

    let key = escape_bq_ident(inputs.unique_key);
    let ts = escape_bq_ident(inputs.updated_at);

    let mut ddl = format!("CREATE TABLE IF NOT EXISTS {table_ref}");
    if auto_optimize {
        ddl.push_str("\nPARTITION BY DATE(dbt_valid_from)");
        ddl.push_str(&format!("\nCLUSTER BY `{}`", key));
    }
    ddl.push_str(&format!(
        " AS\n\
         SELECT src.*,\n\
         TO_HEX(MD5(CAST(src.`{key}` AS STRING) || '|' || CAST(src.`{ts}` AS STRING))) AS dbt_scd_id,\n\
         src.`{ts}` AS dbt_updated_at,\n\
         CURRENT_TIMESTAMP() AS dbt_valid_from,\n\
         CAST(NULL AS TIMESTAMP) AS dbt_valid_to\n\
         FROM ({source}) src",
        key = key,
        ts = ts,
        source = inputs.source_sql,
    ));
    ddl
}

/// Step 1: Invalidate changed rows — set dbt_valid_to on rows whose source has newer data.
pub fn generate_invalidate_sql(inputs: &SnapshotInputs<'_>) -> String {
    let table_ref = bq_table_ref(inputs.project, inputs.dataset, inputs.name);
    let key = escape_bq_ident(inputs.unique_key);
    let ts = escape_bq_ident(inputs.updated_at);
    format!(
        "UPDATE {table_ref} snap\n\
         SET snap.dbt_valid_to = CURRENT_TIMESTAMP()\n\
         WHERE snap.dbt_valid_to IS NULL\n\
         AND snap.`{key}` IN (\n\
           SELECT src.`{key}` FROM ({source}) src\n\
           WHERE src.`{ts}` > snap.dbt_updated_at\n\
         )",
        key = key,
        ts = ts,
        source = inputs.source_sql,
    )
}

/// Step 2: Insert new versions of changed/new rows.
pub fn generate_insert_sql(inputs: &SnapshotInputs<'_>) -> String {
    let table_ref = bq_table_ref(inputs.project, inputs.dataset, inputs.name);
    let key = escape_bq_ident(inputs.unique_key);
    let ts = escape_bq_ident(inputs.updated_at);
    format!(
        "INSERT INTO {table_ref}\n\
         SELECT src.*,\n\
         TO_HEX(MD5(CAST(src.`{key}` AS STRING) || '|' || CAST(src.`{ts}` AS STRING))) AS dbt_scd_id,\n\
         src.`{ts}` AS dbt_updated_at,\n\
         CURRENT_TIMESTAMP() AS dbt_valid_from,\n\
         CAST(NULL AS TIMESTAMP) AS dbt_valid_to\n\
         FROM ({source}) src\n\
         LEFT JOIN {table_ref} snap\n\
           ON src.`{key}` = snap.`{key}` AND snap.dbt_valid_to IS NULL\n\
         WHERE snap.`{key}` IS NULL OR src.`{ts}` > snap.dbt_updated_at",
        key = key,
        ts = ts,
        source = inputs.source_sql,
    )
}

/// Step 3 (optional): Invalidate hard-deleted rows no longer in source.
pub fn generate_hard_delete_sql(inputs: &SnapshotInputs<'_>) -> String {
    let table_ref = bq_table_ref(inputs.project, inputs.dataset, inputs.name);
    let key = escape_bq_ident(inputs.unique_key);
    format!(
        "UPDATE {table_ref} snap\n\
         SET snap.dbt_valid_to = CURRENT_TIMESTAMP()\n\
         WHERE snap.dbt_valid_to IS NULL\n\
         AND snap.`{key}` NOT IN (\n\
           SELECT src.`{key}` FROM ({source}) src\n\
         )",
        key = key,
        source = inputs.source_sql,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::types::SnapshotInputs;

    fn base_inputs<'a>() -> SnapshotInputs<'a> {
        SnapshotInputs {
            project: "proj",
            region: "us-central1",
            dataset: "snapshots",
            name: "snap_customers",
            source_sql: "SELECT * FROM `proj.analytics.dim_customers`",
            unique_key: "customer_id",
            strategy: "timestamp",
            updated_at: "updated_at",
            schedule: "0 2 * * *",
            time_zone: "UTC",
            service_account: "sa@iam",
            description: None,
            paused: None,
            invalidate_hard_deletes: None,
            auto_optimize: None,
            source_schema: None,
        }
    }

    #[test]
    fn create_ddl_contains_scd_columns() {
        let inputs = base_inputs();
        let ddl = generate_snapshot_create_ddl(&inputs);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS `proj.snapshots.snap_customers`"));
        assert!(ddl.contains("dbt_scd_id"));
        assert!(ddl.contains("dbt_updated_at"));
        assert!(ddl.contains("dbt_valid_from"));
        assert!(ddl.contains("dbt_valid_to"));
        assert!(ddl.contains("CAST(NULL AS TIMESTAMP)"));
    }

    #[test]
    fn create_ddl_auto_optimize_default() {
        let inputs = base_inputs();
        let ddl = generate_snapshot_create_ddl(&inputs);
        assert!(ddl.contains("PARTITION BY DATE(dbt_valid_from)"));
        assert!(ddl.contains("CLUSTER BY `customer_id`"));
    }

    #[test]
    fn create_ddl_auto_optimize_true() {
        let mut inputs = base_inputs();
        inputs.auto_optimize = Some(true);
        let ddl = generate_snapshot_create_ddl(&inputs);
        assert!(ddl.contains("PARTITION BY DATE(dbt_valid_from)"));
        assert!(ddl.contains("CLUSTER BY `customer_id`"));
    }

    #[test]
    fn create_ddl_auto_optimize_false() {
        let mut inputs = base_inputs();
        inputs.auto_optimize = Some(false);
        let ddl = generate_snapshot_create_ddl(&inputs);
        assert!(!ddl.contains("PARTITION BY"));
        assert!(!ddl.contains("CLUSTER BY"));
    }

    #[test]
    fn invalidate_sql_targets_changed_rows() {
        let inputs = base_inputs();
        let sql = generate_invalidate_sql(&inputs);
        assert!(sql.contains("UPDATE `proj.snapshots.snap_customers` snap"));
        assert!(sql.contains("SET snap.dbt_valid_to = CURRENT_TIMESTAMP()"));
        assert!(sql.contains("WHERE snap.dbt_valid_to IS NULL"));
        assert!(sql.contains("src.`updated_at` > snap.dbt_updated_at"));
    }

    #[test]
    fn insert_sql_joins_on_key() {
        let inputs = base_inputs();
        let sql = generate_insert_sql(&inputs);
        assert!(sql.contains("INSERT INTO `proj.snapshots.snap_customers`"));
        assert!(sql.contains("ON src.`customer_id` = snap.`customer_id`"));
        assert!(sql.contains("snap.dbt_valid_to IS NULL"));
    }

    #[test]
    fn hard_delete_sql_invalidates_missing() {
        let inputs = base_inputs();
        let sql = generate_hard_delete_sql(&inputs);
        assert!(sql.contains("NOT IN"));
        assert!(sql.contains("dbt_valid_to = CURRENT_TIMESTAMP()"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn snapshot_ddl_never_panics(
            project in "[a-z][a-z0-9-]{0,20}",
            dataset in "[a-z][a-z0-9_]{0,20}",
            name in "[a-z][a-z0-9_]{0,20}",
            source_sql in "[ -~]{0,200}",
            unique_key in "[a-z][a-z0-9_]{0,20}",
            updated_at in "[a-z][a-z0-9_]{0,20}",
        ) {
            let inputs = SnapshotInputs {
                project: &project,
                region: "us-central1",
                dataset: &dataset,
                name: &name,
                source_sql: &source_sql,
                unique_key: &unique_key,
                strategy: "timestamp",
                updated_at: &updated_at,
                schedule: "0 2 * * *",
                time_zone: "UTC",
                service_account: "sa@iam",
                description: None,
                paused: None,
                invalidate_hard_deletes: Some(true),
                auto_optimize: None,
                source_schema: None,
            };
            let _ = std::hint::black_box(generate_snapshot_create_ddl(&inputs));
            let _ = std::hint::black_box(generate_invalidate_sql(&inputs));
            let _ = std::hint::black_box(generate_insert_sql(&inputs));
            let _ = std::hint::black_box(generate_hard_delete_sql(&inputs));
        }
    }
}
