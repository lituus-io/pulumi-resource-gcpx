use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::BqOps;
use crate::dataproc::job_lifecycle::delete_scheduler_and_workflow;
use crate::dataproc::scheduler_body::{
    build_scheduler_create_body, build_scheduler_patch_body, SchedulerBodyConfig,
};
use crate::diff_macro::diff_fields;
use crate::error::IntoStatusWith;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::prost_util::get_str;
use crate::provider::GcpxProvider;
use crate::resource::require_non_empty;
use crate::scheduler_ops::SchedulerOps;
use crate::schema::types::CheckFailure;
use crate::snapshot::ddl::generate_snapshot_create_ddl;
use crate::snapshot::parse::{build_snapshot_output, parse_snapshot_inputs};
use crate::snapshot::types::SnapshotState;
use crate::snapshot::workflow::generate_snapshot_workflow_yaml;

fn validate_snapshot(inputs: &crate::snapshot::types::SnapshotInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "region", inputs.region);
    require_non_empty(&mut failures, "dataset", inputs.dataset);
    require_non_empty(&mut failures, "name", inputs.name);
    require_non_empty(&mut failures, "sourceSql", inputs.source_sql);
    require_non_empty(&mut failures, "uniqueKey", inputs.unique_key);
    require_non_empty(&mut failures, "updatedAt", inputs.updated_at);
    require_non_empty(&mut failures, "schedule", inputs.schedule);
    require_non_empty(&mut failures, "serviceAccount", inputs.service_account);

    if !matches!(inputs.strategy, "timestamp") {
        failures.push(CheckFailure {
            property: "strategy".into(),
            reason:
                "strategy must be 'timestamp' (the only currently supported SCD Type 2 strategy)"
                    .into(),
        });
    }

    failures
}

impl<C: BqOps + SchedulerOps> GcpxProvider<C> {
    pub async fn check_snapshot(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_snapshot_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_snapshot(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_snapshot(
        &self,
        req: pulumirpc::DiffRequest,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let olds = req
            .olds
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing olds"))?;
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let old_inputs = parse_snapshot_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_snapshot_inputs(news).map_err(Status::invalid_argument)?;

        let mut replace_keys = Vec::new();
        let mut update_keys = Vec::new();

        diff_fields!(old_inputs, new_inputs, replace_keys, update_keys;
            project => replace,
            region => replace,
            name => replace,
            dataset => replace,
            service_account => replace "serviceAccount",
            unique_key => replace "uniqueKey",
            strategy => replace,
            updated_at => replace "updatedAt",
            auto_optimize => replace "autoOptimize",
            source_sql => update "sourceSql",
            schedule => update,
            time_zone => update "timeZone",
            paused => update,
            description => update,
            invalidate_hard_deletes => update "invalidateHardDeletes",
            source_schema => update "sourceSchema",
        );

        Ok(build_diff_response(&replace_keys, &update_keys))
    }

    pub async fn create_snapshot(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_snapshot_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!(
            "snapshot/{}/{}/{}/{}",
            inputs.project, inputs.region, inputs.dataset, inputs.name
        );

        let wf_name = format!("gcpx-snap-wf-{}", inputs.name);
        let sched_name = format!("gcpx-snap-sched-{}", inputs.name);
        let snapshot_table = format!("{}.{}.{}", inputs.project, inputs.dataset, inputs.name);

        if req.preview {
            let state = SnapshotState {
                workflow_name: format!(
                    "projects/{}/locations/{}/workflows/{}",
                    inputs.project, inputs.region, wf_name
                ),
                scheduler_job_name: format!(
                    "projects/{}/locations/{}/jobs/{}",
                    inputs.project, inputs.region, sched_name
                ),
                snapshot_table,
                state: "PREVIEW".to_owned(),
                next_run_time: String::new(),
            };
            let outputs = build_snapshot_output(&inputs, &state);
            return Ok(Response::new(pulumirpc::CreateResponse {
                id,
                properties: Some(outputs),
                ..Default::default()
            }));
        }

        // 1. Create the snapshot table via DDL.
        let create_ddl = generate_snapshot_create_ddl(&inputs);
        self.client
            .execute_ddl(inputs.project, &create_ddl, None)
            .await
            .status_internal_with("failed to create snapshot table")?;

        // 2. Generate and create the workflow (with 409 auto-adopt).
        let workflow_yaml = generate_snapshot_workflow_yaml(&inputs);
        let wf_meta = create_or_adopt(
            self.client.create_workflow(
                inputs.project,
                inputs.region,
                &wf_name,
                &workflow_yaml,
                inputs.service_account,
            ),
            || {
                self.client
                    .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
            },
            "snapshot workflow",
        )
        .await?;

        // 3. Build and create the scheduler job (with 409 auto-adopt).
        let sched_cfg = SchedulerBodyConfig {
            project: inputs.project,
            region: inputs.region,
            sched_name: &sched_name,
            wf_name: &wf_name,
            schedule: inputs.schedule,
            time_zone: inputs.time_zone,
            service_account: inputs.service_account,
            description: inputs.description,
            paused: inputs.paused,
            retry_count: None,
            attempt_deadline: None,
        };
        let sched_body = build_scheduler_create_body(&sched_cfg);

        let sched_meta = match create_or_adopt(
            self.client
                .create_scheduler_job(inputs.project, inputs.region, &sched_body),
            || {
                self.client.patch_scheduler_job(
                    inputs.project,
                    inputs.region,
                    &sched_name,
                    &sched_body,
                )
            },
            "snapshot scheduler",
        )
        .await
        {
            Ok(m) => m,
            Err(status) => {
                // Rollback: delete workflow (keep snapshot table — data preservation).
                let _ = self
                    .client
                    .delete_workflow(inputs.project, inputs.region, &wf_name)
                    .await;
                return Err(status);
            }
        };

        let state = SnapshotState {
            workflow_name: wf_meta.name,
            scheduler_job_name: sched_meta.name,
            snapshot_table,
            state: sched_meta.state,
            next_run_time: sched_meta.schedule_time,
        };
        let outputs = build_snapshot_output(&inputs, &state);
        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_snapshot(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: req.properties,
            inputs: req.inputs,
            ..Default::default()
        }))
    }

    pub async fn update_snapshot(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_snapshot_inputs(news).map_err(Status::invalid_argument)?;
        let wf_name = format!("gcpx-snap-wf-{}", inputs.name);
        let sched_name = format!("gcpx-snap-sched-{}", inputs.name);
        let snapshot_table = format!("{}.{}.{}", inputs.project, inputs.dataset, inputs.name);

        if !req.preview {
            // Check if source SQL or hard delete flag changed -> update workflow.
            let old_source = req
                .olds
                .as_ref()
                .and_then(|o| get_str(&o.fields, "sourceSql"))
                .unwrap_or("");
            let old_hard_deletes = req
                .olds
                .as_ref()
                .and_then(|o| crate::prost_util::get_bool(&o.fields, "invalidateHardDeletes"));
            let old_source_schema = req
                .olds
                .as_ref()
                .and_then(|o| get_str(&o.fields, "sourceSchema"));

            // Schema adaptation: when sourceSchema fingerprint changes, add new columns.
            let schema_changed =
                old_source_schema != inputs.source_schema && inputs.source_schema.is_some();
            if schema_changed {
                let scd_cols = [
                    "dbt_scd_id",
                    "dbt_updated_at",
                    "dbt_valid_from",
                    "dbt_valid_to",
                ];
                let dry_run = self
                    .client
                    .dry_run_query(inputs.project, inputs.source_sql, None)
                    .await
                    .status_internal_with("failed to dry-run sourceSql for schema adaptation")?;
                let snap_schema = self
                    .client
                    .get_table_schema(inputs.project, inputs.dataset, inputs.name)
                    .await
                    .status_internal_with("failed to get snapshot table schema")?;
                let new_cols: Vec<_> = dry_run
                    .schema
                    .iter()
                    .filter(|f| {
                        !snap_schema
                            .iter()
                            .any(|s| s.name.eq_ignore_ascii_case(&f.name))
                    })
                    .filter(|f| !scd_cols.iter().any(|s| f.name.eq_ignore_ascii_case(s)))
                    .collect();
                for col in &new_cols {
                    let alter_ddl = format!(
                        "ALTER TABLE `{}.{}.{}` ADD COLUMN `{}` {}",
                        inputs.project, inputs.dataset, inputs.name, col.name, col.field_type,
                    );
                    self.client
                        .execute_ddl(inputs.project, &alter_ddl, None)
                        .await
                        .status_internal_with("failed to add column to snapshot table")?;
                }
            }

            let workflow_changed = old_source != inputs.source_sql
                || old_hard_deletes != inputs.invalidate_hard_deletes
                || schema_changed;
            if workflow_changed {
                let workflow_yaml = generate_snapshot_workflow_yaml(&inputs);
                self.client
                    .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
                    .await
                    .status_internal_with("failed to update snapshot workflow")?;
            }

            // Patch scheduler job (no retry fields, no state — use dedicated API).
            let sched_cfg = SchedulerBodyConfig {
                project: inputs.project,
                region: inputs.region,
                sched_name: &sched_name,
                wf_name: &wf_name,
                schedule: inputs.schedule,
                time_zone: inputs.time_zone,
                service_account: inputs.service_account,
                description: inputs.description,
                paused: inputs.paused,
                retry_count: None,
                attempt_deadline: None,
            };
            let patch = build_scheduler_patch_body(&sched_cfg, false);

            self.client
                .patch_scheduler_job(inputs.project, inputs.region, &sched_name, &patch)
                .await
                .status_internal_with("failed to update snapshot scheduler")?;

            // Cloud Scheduler ignores state in PATCH — use dedicated pause/resume API.
            match inputs.paused {
                Some(true) => {
                    self.client
                        .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
                        .await
                        .status_internal_with("failed to pause snapshot scheduler")?;
                }
                Some(false) => {
                    self.client
                        .resume_scheduler_job(inputs.project, inputs.region, &sched_name)
                        .await
                        .status_internal_with("failed to resume snapshot scheduler")?;
                }
                None => {}
            }
        }

        let state = SnapshotState {
            workflow_name: format!(
                "projects/{}/locations/{}/workflows/{}",
                inputs.project, inputs.region, wf_name
            ),
            scheduler_job_name: format!(
                "projects/{}/locations/{}/jobs/{}",
                inputs.project, inputs.region, sched_name
            ),
            snapshot_table,
            state: if inputs.paused == Some(true) {
                "PAUSED"
            } else {
                "ENABLED"
            }
            .to_owned(),
            next_run_time: String::new(),
        };
        let outputs = build_snapshot_output(&inputs, &state);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_snapshot(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        if let Some(ref props) = req.properties {
            let project = get_str(&props.fields, "project").unwrap_or("");
            let region = get_str(&props.fields, "region").unwrap_or("");
            let name = get_str(&props.fields, "name").unwrap_or("");

            if !project.is_empty() && !region.is_empty() && !name.is_empty() {
                // Intentionally keep the snapshot TABLE (data preservation).
                let sched_name = format!("gcpx-snap-sched-{}", name);
                let wf_name = format!("gcpx-snap-wf-{}", name);
                return delete_scheduler_and_workflow(
                    &self.client,
                    project,
                    region,
                    &sched_name,
                    &wf_name,
                )
                .await;
            }
        }

        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp_client::MockGcpClient;
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

    fn mock_provider() -> GcpxProvider<MockGcpClient> {
        GcpxProvider::new(MockGcpClient::new(vec![]))
    }

    #[tokio::test]
    async fn check_valid_snapshot() {
        let p = mock_provider();
        let resp = p
            .check_snapshot(pulumirpc::CheckRequest {
                urn: "urn:pulumi:stack::proj::gcpx:dbt/snapshot:Snapshot::test".into(),
                news: Some(make_snapshot_struct()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_invalid_strategy() {
        let p = mock_provider();
        let mut s = make_snapshot_struct();
        s.fields
            .insert("strategy".to_owned(), prost_string("check"));
        let resp = p
            .check_snapshot(pulumirpc::CheckRequest {
                urn: "test".into(),
                news: Some(s),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(failures.iter().any(|f| f.reason.contains("timestamp")));
    }

    #[tokio::test]
    async fn diff_no_changes() {
        let p = mock_provider();
        let s = make_snapshot_struct();
        let resp = p
            .diff_snapshot(pulumirpc::DiffRequest {
                id: "test".into(),
                urn: "test".into(),
                olds: Some(s.clone()),
                news: Some(s),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().changes,
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        );
    }

    #[tokio::test]
    async fn diff_name_triggers_replace() {
        let p = mock_provider();
        let olds = make_snapshot_struct();
        let mut news = make_snapshot_struct();
        news.fields
            .insert("name".to_owned(), prost_string("snap_new"));
        let resp = p
            .diff_snapshot(pulumirpc::DiffRequest {
                id: "test".into(),
                urn: "test".into(),
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.replaces.contains(&"name".to_string()));
    }

    #[tokio::test]
    async fn diff_schedule_triggers_update() {
        let p = mock_provider();
        let olds = make_snapshot_struct();
        let mut news = make_snapshot_struct();
        news.fields
            .insert("schedule".to_owned(), prost_string("0 3 * * *"));
        let resp = p
            .diff_snapshot(pulumirpc::DiffRequest {
                id: "test".into(),
                urn: "test".into(),
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.replaces.is_empty());
        assert!(inner.detailed_diff.contains_key("schedule"));
    }

    #[tokio::test]
    async fn diff_source_schema_triggers_update() {
        let p = mock_provider();
        let olds = make_snapshot_struct();
        let mut news = make_snapshot_struct();
        news.fields
            .insert("sourceSchema".to_owned(), prost_string("fp-v2"));
        let resp = p
            .diff_snapshot(pulumirpc::DiffRequest {
                id: "test".into(),
                urn: "test".into(),
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.replaces.is_empty());
        assert!(inner.detailed_diff.contains_key("sourceSchema"));
    }

    #[tokio::test]
    async fn diff_auto_optimize_triggers_replace() {
        let p = mock_provider();
        let olds = make_snapshot_struct();
        let mut news = make_snapshot_struct();
        news.fields.insert(
            "autoOptimize".to_owned(),
            crate::prost_util::prost_bool(false),
        );
        let resp = p
            .diff_snapshot(pulumirpc::DiffRequest {
                id: "test".into(),
                urn: "test".into(),
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.replaces.contains(&"autoOptimize".to_string()));
    }

    #[tokio::test]
    async fn create_preview() {
        let p = mock_provider();
        let resp = p
            .create_snapshot(pulumirpc::CreateRequest {
                urn: "test".into(),
                properties: Some(make_snapshot_struct()),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.id.starts_with("snapshot/"));
        let props = inner.properties.unwrap();
        assert_eq!(get_str(&props.fields, "state"), Some("PREVIEW"));
        assert_eq!(get_str(&props.fields, "tableId"), Some("snap_dim"));
    }
}
