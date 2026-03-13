use crate::output::OutputBuilder;
use crate::prost_util::{get_bool, get_str};
use crate::snapshot::types::{SnapshotInputs, SnapshotState};

pub fn parse_snapshot_inputs(s: &prost_types::Struct) -> Result<SnapshotInputs<'_>, &'static str> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': GCP project for snapshot resources")?;
    let region = get_str(&s.fields, "region").ok_or("missing required field 'region': GCP region for workflows and scheduler (e.g., 'us-central1')")?;
    let dataset = get_str(&s.fields, "dataset")
        .ok_or("missing required field 'dataset': BigQuery dataset for the snapshot table")?;
    let name =
        get_str(&s.fields, "name").ok_or("missing required field 'name': snapshot table name")?;
    let source_sql = get_str(&s.fields, "sourceSql").ok_or(
        "missing required field 'sourceSql': SELECT query that defines what data to snapshot",
    )?;
    let unique_key = get_str(&s.fields, "uniqueKey").ok_or("missing required field 'uniqueKey': column that uniquely identifies rows (e.g., 'customer_id')")?;
    let strategy = get_str(&s.fields, "strategy").unwrap_or("timestamp");
    let updated_at = get_str(&s.fields, "updatedAt").ok_or("missing required field 'updatedAt': timestamp column for change detection (e.g., 'updated_at')")?;
    let schedule = get_str(&s.fields, "schedule").ok_or(
        "missing required field 'schedule': cron expression (e.g., '0 2 * * *' for daily at 2 AM)",
    )?;
    let time_zone = get_str(&s.fields, "timeZone").unwrap_or("UTC");
    let service_account = get_str(&s.fields, "serviceAccount").ok_or(
        "missing required field 'serviceAccount': service account email for job execution",
    )?;
    let description = get_str(&s.fields, "description");
    let paused = get_bool(&s.fields, "paused");
    let invalidate_hard_deletes = get_bool(&s.fields, "invalidateHardDeletes");
    let auto_optimize = get_bool(&s.fields, "autoOptimize");
    let source_schema = get_str(&s.fields, "sourceSchema");

    Ok(SnapshotInputs {
        project,
        region,
        dataset,
        name,
        source_sql,
        unique_key,
        strategy,
        updated_at,
        schedule,
        time_zone,
        service_account,
        description,
        paused,
        invalidate_hard_deletes,
        auto_optimize,
        source_schema,
    })
}

pub fn build_snapshot_output(
    inputs: &SnapshotInputs<'_>,
    state: &SnapshotState,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("region", inputs.region)
        .str("dataset", inputs.dataset)
        .str("name", inputs.name)
        .str("sourceSql", inputs.source_sql)
        .str("uniqueKey", inputs.unique_key)
        .str("strategy", inputs.strategy)
        .str("updatedAt", inputs.updated_at)
        .str("schedule", inputs.schedule)
        .str("timeZone", inputs.time_zone)
        .str("serviceAccount", inputs.service_account)
        .str("tableId", inputs.name)
        .str("snapshotTable", &state.snapshot_table)
        .str("workflowName", &state.workflow_name)
        .str("schedulerJobName", &state.scheduler_job_name)
        .str("state", &state.state)
        .str("nextRunTime", &state.next_run_time)
        .str_opt("description", inputs.description)
        .bool_opt("paused", inputs.paused)
        .bool_opt("invalidateHardDeletes", inputs.invalidate_hard_deletes)
        .bool_opt("autoOptimize", inputs.auto_optimize)
        .str_opt("sourceSchema", inputs.source_schema)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_util::prost_string;

    fn make_snapshot_struct() -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string("proj")),
                ("region".to_owned(), prost_string("us-central1")),
                ("dataset".to_owned(), prost_string("snapshots")),
                ("name".to_owned(), prost_string("snap_dim")),
                ("sourceSql".to_owned(), prost_string("SELECT * FROM t")),
                ("uniqueKey".to_owned(), prost_string("id")),
                ("strategy".to_owned(), prost_string("timestamp")),
                ("updatedAt".to_owned(), prost_string("updated_at")),
                ("schedule".to_owned(), prost_string("0 2 * * *")),
                ("timeZone".to_owned(), prost_string("UTC")),
                ("serviceAccount".to_owned(), prost_string("sa@iam")),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn parse_all_required_fields() {
        let s = make_snapshot_struct();
        let inputs = parse_snapshot_inputs(&s).unwrap();
        assert_eq!(inputs.project, "proj");
        assert_eq!(inputs.region, "us-central1");
        assert_eq!(inputs.dataset, "snapshots");
        assert_eq!(inputs.name, "snap_dim");
        assert_eq!(inputs.source_sql, "SELECT * FROM t");
        assert_eq!(inputs.unique_key, "id");
        assert_eq!(inputs.updated_at, "updated_at");
        assert_eq!(inputs.schedule, "0 2 * * *");
        assert_eq!(inputs.service_account, "sa@iam");
    }

    #[test]
    fn parse_missing_required() {
        let s = prost_types::Struct {
            fields: vec![("project".to_owned(), prost_string("proj"))]
                .into_iter()
                .collect(),
        };
        let err = parse_snapshot_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'region'"));
    }

    #[test]
    fn build_output_includes_state() {
        let inputs = SnapshotInputs {
            project: "proj",
            region: "us-central1",
            dataset: "snapshots",
            name: "snap_dim",
            source_sql: "SELECT 1",
            unique_key: "id",
            strategy: "timestamp",
            updated_at: "updated_at",
            schedule: "0 2 * * *",
            time_zone: "UTC",
            service_account: "sa@iam",
            description: Some("test snap"),
            paused: None,
            invalidate_hard_deletes: Some(true),
            auto_optimize: None,
            source_schema: None,
        };
        let state = SnapshotState {
            workflow_name: "wf-snap".to_owned(),
            scheduler_job_name: "sched-snap".to_owned(),
            snapshot_table: "proj.snapshots.snap_dim".to_owned(),
            state: "ENABLED".to_owned(),
            next_run_time: "2026-01-01T00:00:00Z".to_owned(),
        };
        let output = build_snapshot_output(&inputs, &state);
        assert_eq!(get_str(&output.fields, "tableId"), Some("snap_dim"));
        assert_eq!(
            get_str(&output.fields, "snapshotTable"),
            Some("proj.snapshots.snap_dim")
        );
        assert_eq!(get_str(&output.fields, "workflowName"), Some("wf-snap"));
        assert_eq!(get_str(&output.fields, "description"), Some("test snap"));
        assert_eq!(
            get_bool(&output.fields, "invalidateHardDeletes"),
            Some(true)
        );
    }
}
