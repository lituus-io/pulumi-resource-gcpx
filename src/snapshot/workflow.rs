use crate::snapshot::ddl;
use crate::snapshot::types::SnapshotInputs;

/// Generate a multi-step GCP Workflow YAML for SCD Type 2 snapshot.
///
/// Steps:
///   1. Invalidate changed rows (UPDATE SET dbt_valid_to)
///   2. Insert new versions of changed/new rows
///   3. (Optional) Invalidate hard-deleted rows
pub fn generate_snapshot_workflow_yaml(inputs: &SnapshotInputs<'_>) -> String {
    let invalidate_sql = ddl::generate_invalidate_sql(inputs);
    let insert_sql = ddl::generate_insert_sql(inputs);

    let mut yaml = String::with_capacity(2048);

    yaml.push_str(&format_step(
        "invalidateChanged",
        inputs.project,
        &invalidate_sql,
    ));
    yaml.push('\n');
    yaml.push_str(&format_step(
        "insertNewVersions",
        inputs.project,
        &insert_sql,
    ));

    if inputs.invalidate_hard_deletes == Some(true) {
        let hard_delete_sql = ddl::generate_hard_delete_sql(inputs);
        yaml.push('\n');
        yaml.push_str(&format_step(
            "invalidateHardDeletes",
            inputs.project,
            &hard_delete_sql,
        ));
    }

    yaml
}

fn format_step(step_name: &str, project: &str, sql: &str) -> String {
    let escaped = escape_sql_for_yaml(sql);
    format!(
        "- {step_name}:\n\
         \x20   call: googleapis.bigquery.v2.jobs.query\n\
         \x20   args:\n\
         \x20       projectId: {project}\n\
         \x20       body:\n\
         \x20           useLegacySql: false\n\
         \x20           useQueryCache: false\n\
         \x20           timeoutMs: 600000\n\
         \x20           query: |\n\
         \x20               {escaped}",
    )
}

fn escape_sql_for_yaml(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len() + 256);
    for (i, line) in sql.lines().enumerate() {
        if i > 0 {
            result.push('\n');
            result.push_str("                ");
        }
        result.push_str(line);
    }
    result
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
    fn workflow_has_two_steps() {
        let inputs = base_inputs();
        let yaml = generate_snapshot_workflow_yaml(&inputs);
        assert!(yaml.contains("invalidateChanged:"));
        assert!(yaml.contains("insertNewVersions:"));
        assert!(!yaml.contains("invalidateHardDeletes:"));
    }

    #[test]
    fn workflow_with_hard_deletes_has_three_steps() {
        let mut inputs = base_inputs();
        inputs.invalidate_hard_deletes = Some(true);
        let yaml = generate_snapshot_workflow_yaml(&inputs);
        assert!(yaml.contains("invalidateChanged:"));
        assert!(yaml.contains("insertNewVersions:"));
        assert!(yaml.contains("invalidateHardDeletes:"));
    }

    #[test]
    fn workflow_steps_use_bigquery_connector() {
        let inputs = base_inputs();
        let yaml = generate_snapshot_workflow_yaml(&inputs);
        // Each step should use the BQ connector.
        let bq_calls = yaml.matches("googleapis.bigquery.v2.jobs.query").count();
        assert_eq!(bq_calls, 2);
    }

    #[test]
    fn workflow_contains_project() {
        let inputs = base_inputs();
        let yaml = generate_snapshot_workflow_yaml(&inputs);
        assert!(yaml.contains("projectId: proj"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn snapshot_workflow_never_panics(
            project in "[a-z][a-z0-9-]{0,20}",
            dataset in "[a-z][a-z0-9_]{0,20}",
            name in "[a-z][a-z0-9_]{0,20}",
            source_sql in "[ -~]{0,200}",
            unique_key in "[a-z][a-z0-9_]{0,20}",
            updated_at in "[a-z][a-z0-9_]{0,20}",
            hard_deletes in proptest::bool::ANY,
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
                invalidate_hard_deletes: Some(hard_deletes),
                auto_optimize: None,
                source_schema: None,
            };
            let yaml = generate_snapshot_workflow_yaml(&inputs);
            // Must always have the two required steps
            prop_assert!(yaml.contains("invalidateChanged:"));
            prop_assert!(yaml.contains("insertNewVersions:"));
            if hard_deletes {
                prop_assert!(yaml.contains("invalidateHardDeletes:"));
            }
        }
    }
}
