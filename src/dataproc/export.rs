use std::borrow::Cow;

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::dataproc::job_lifecycle::delete_scheduler_and_workflow;
use crate::dataproc::parse::{build_export_output, parse_export_inputs};
use crate::dataproc::scheduler_body::{
    build_scheduler_create_body, build_scheduler_patch_body, SchedulerBodyConfig,
};
use crate::dataproc::types::{DataprocJobState, ExportJobInputs};
use crate::dataproc::workflow_template::{build_export_args, generate_dataproc_workflow_yaml};
use crate::error::IntoStatusWith;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::prost_util::get_str;
use crate::provider::GcpxProvider;
use crate::resource::require_non_empty;
use crate::scheduler_ops::SchedulerOps;
use crate::schema::types::CheckFailure;

fn validate_export(inputs: &ExportJobInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "region", inputs.region);
    require_non_empty(&mut failures, "name", inputs.name);
    require_non_empty(&mut failures, "sourceDataset", inputs.source_dataset);
    require_non_empty(&mut failures, "sourceTable", inputs.source_table);
    require_non_empty(&mut failures, "connectionString", inputs.connection_string);
    require_non_empty(&mut failures, "secret", inputs.secret);
    require_non_empty(&mut failures, "destTable", inputs.dest_table);
    require_non_empty(&mut failures, "imageUri", inputs.image_uri);
    require_non_empty(&mut failures, "scriptUri", inputs.script_uri);
    require_non_empty(&mut failures, "serviceAccount", inputs.service_account);
    require_non_empty(&mut failures, "stagingBucket", inputs.staging_bucket);
    require_non_empty(&mut failures, "processBucket", inputs.process_bucket);
    require_non_empty(&mut failures, "subnetworkUri", inputs.subnetwork_uri);
    require_non_empty(&mut failures, "schedule", inputs.schedule);

    // JDBC prefix must match database type.
    if !inputs
        .connection_string
        .starts_with(inputs.database_type.jdbc_prefix())
    {
        failures.push(CheckFailure {
            property: Cow::Borrowed("connectionString"),
            reason: Cow::Owned(format!(
                "connectionString must start with '{}' for databaseType '{}'",
                inputs.database_type.jdbc_prefix(),
                inputs.database_type.label(),
            )),
        });
    }

    if let Some(rc) = inputs.retry_count {
        if !(0..=5).contains(&rc) {
            failures.push(CheckFailure {
                property: Cow::Borrowed("retryCount"),
                reason: Cow::Borrowed("retryCount must be 0-5 (inclusive)"),
            });
        }
    }

    failures
}

fn sched_config<'a>(
    inputs: &'a ExportJobInputs<'a>,
    sched_name: &'a str,
    wf_name: &'a str,
) -> SchedulerBodyConfig<'a> {
    SchedulerBodyConfig {
        project: inputs.project,
        region: inputs.region,
        sched_name,
        wf_name,
        schedule: inputs.schedule,
        time_zone: inputs.time_zone,
        service_account: inputs.service_account,
        description: inputs.description,
        paused: inputs.paused,
        retry_count: inputs.retry_count,
        attempt_deadline: inputs.attempt_deadline,
    }
}

fn generate_export_workflow(inputs: &ExportJobInputs<'_>) -> String {
    let args = build_export_args(
        inputs.process_bucket,
        inputs.source_dataset,
        inputs.source_table,
        inputs.connection_string,
        inputs.secret,
        inputs.dest_table,
    );

    generate_dataproc_workflow_yaml(
        inputs.project,
        inputs.region,
        inputs.name,
        inputs.image_uri,
        inputs.script_uri,
        &inputs.jar_uris,
        &args,
        inputs.runtime_version,
        inputs.service_account,
        inputs.staging_bucket,
        inputs.subnetwork_uri,
        &inputs.network_tags,
        true, // Export always reads from BigQuery
    )
}

impl<C: SchedulerOps> GcpxProvider<C> {
    pub async fn check_export_job(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_export_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_export(&inputs);
        build_check_response(req.news, failures)
    }

    pub async fn diff_export_job(
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

        let old_inputs = parse_export_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_export_inputs(news).map_err(Status::invalid_argument)?;

        let mut replace_keys = Vec::new();
        let mut update_keys = Vec::new();

        // Replace triggers.
        if old_inputs.project != new_inputs.project {
            replace_keys.push("project");
        }
        if old_inputs.region != new_inputs.region {
            replace_keys.push("region");
        }
        if old_inputs.name != new_inputs.name {
            replace_keys.push("name");
        }
        if old_inputs.service_account != new_inputs.service_account {
            replace_keys.push("serviceAccount");
        }
        if old_inputs.database_type != new_inputs.database_type {
            replace_keys.push("databaseType");
        }
        if old_inputs.source_dataset != new_inputs.source_dataset {
            replace_keys.push("sourceDataset");
        }

        // Update triggers.
        if old_inputs.source_table != new_inputs.source_table {
            update_keys.push("sourceTable");
        }
        if old_inputs.connection_string != new_inputs.connection_string {
            update_keys.push("connectionString");
        }
        if old_inputs.secret != new_inputs.secret {
            update_keys.push("secret");
        }
        if old_inputs.dest_table != new_inputs.dest_table {
            update_keys.push("destTable");
        }
        if old_inputs.image_uri != new_inputs.image_uri {
            update_keys.push("imageUri");
        }
        if old_inputs.script_uri != new_inputs.script_uri {
            update_keys.push("scriptUri");
        }
        if old_inputs.jar_uris != new_inputs.jar_uris {
            update_keys.push("jarUris");
        }
        if old_inputs.staging_bucket != new_inputs.staging_bucket {
            update_keys.push("stagingBucket");
        }
        if old_inputs.process_bucket != new_inputs.process_bucket {
            update_keys.push("processBucket");
        }
        if old_inputs.subnetwork_uri != new_inputs.subnetwork_uri {
            update_keys.push("subnetworkUri");
        }
        if old_inputs.network_tags != new_inputs.network_tags {
            update_keys.push("networkTags");
        }
        if old_inputs.runtime_version != new_inputs.runtime_version {
            update_keys.push("runtimeVersion");
        }
        if old_inputs.schedule != new_inputs.schedule {
            update_keys.push("schedule");
        }
        if old_inputs.time_zone != new_inputs.time_zone {
            update_keys.push("timeZone");
        }
        if old_inputs.paused != new_inputs.paused {
            update_keys.push("paused");
        }
        if old_inputs.description != new_inputs.description {
            update_keys.push("description");
        }
        if old_inputs.retry_count != new_inputs.retry_count {
            update_keys.push("retryCount");
        }
        if old_inputs.attempt_deadline != new_inputs.attempt_deadline {
            update_keys.push("attemptDeadline");
        }

        Ok(build_diff_response(&replace_keys, &update_keys))
    }

    pub async fn create_export_job(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_export_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!(
            "export-job/{}/{}/{}",
            inputs.project, inputs.region, inputs.name
        );
        let wf_name = format!("gcpx-wf-export-{}", inputs.name);
        let sched_name = format!("gcpx-sched-export-{}", inputs.name);

        if req.preview {
            let state = DataprocJobState {
                workflow_name: format!(
                    "projects/{}/locations/{}/workflows/{}",
                    inputs.project, inputs.region, wf_name
                ),
                scheduler_job_name: format!(
                    "projects/{}/locations/{}/jobs/{}",
                    inputs.project, inputs.region, sched_name
                ),
                state: "PREVIEW".to_owned(),
                next_run_time: String::new(),
            };
            let outputs = build_export_output(&inputs, &state);
            return Ok(Response::new(pulumirpc::CreateResponse {
                id,
                properties: Some(outputs),
                ..Default::default()
            }));
        }

        let workflow_yaml = generate_export_workflow(&inputs);

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
            "workflow",
        )
        .await?;

        let sched_cfg = sched_config(&inputs, &sched_name, &wf_name);
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
            "scheduler job",
        )
        .await
        {
            Ok(m) => m,
            Err(status) => {
                let _ = self
                    .client
                    .delete_workflow(inputs.project, inputs.region, &wf_name)
                    .await;
                return Err(status);
            }
        };

        // If paused=true, pause the scheduler (Cloud Scheduler ignores state in create body).
        let sched_meta = if inputs.paused == Some(true) {
            self.client
                .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
                .await
                .status_internal_with("failed to pause scheduler job")?
        } else {
            sched_meta
        };

        let state = DataprocJobState {
            workflow_name: wf_meta.name,
            scheduler_job_name: sched_meta.name,
            state: sched_meta.state,
            next_run_time: sched_meta.schedule_time,
        };
        let outputs = build_export_output(&inputs, &state);
        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_export_job(
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

    pub async fn update_export_job(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_export_inputs(news).map_err(Status::invalid_argument)?;
        let wf_name = format!("gcpx-wf-export-{}", inputs.name);
        let sched_name = format!("gcpx-sched-export-{}", inputs.name);

        if !req.preview {
            let workflow_yaml = generate_export_workflow(&inputs);
            self.client
                .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
                .await
                .status_internal_with("failed to update workflow")?;

            let sched_cfg = sched_config(&inputs, &sched_name, &wf_name);
            let patch = build_scheduler_patch_body(&sched_cfg, true);

            self.client
                .patch_scheduler_job(inputs.project, inputs.region, &sched_name, &patch)
                .await
                .status_internal_with("failed to update scheduler job")?;

            // Cloud Scheduler ignores state in PATCH — use dedicated pause/resume API.
            match inputs.paused {
                Some(true) => {
                    self.client
                        .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
                        .await
                        .status_internal_with("failed to pause scheduler job")?;
                }
                Some(false) => {
                    self.client
                        .resume_scheduler_job(inputs.project, inputs.region, &sched_name)
                        .await
                        .status_internal_with("failed to resume scheduler job")?;
                }
                None => {}
            }
        }

        let state = DataprocJobState {
            workflow_name: format!(
                "projects/{}/locations/{}/workflows/{}",
                inputs.project, inputs.region, wf_name
            ),
            scheduler_job_name: format!(
                "projects/{}/locations/{}/jobs/{}",
                inputs.project, inputs.region, sched_name
            ),
            state: if inputs.paused == Some(true) {
                "PAUSED"
            } else {
                "ENABLED"
            }
            .to_owned(),
            next_run_time: String::new(),
        };
        let outputs = build_export_output(&inputs, &state);
        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_export_job(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        if let Some(ref props) = req.properties {
            let project = get_str(&props.fields, "project").unwrap_or("");
            let region = get_str(&props.fields, "region").unwrap_or("");
            let name = get_str(&props.fields, "name").unwrap_or("");

            if !project.is_empty() && !region.is_empty() && !name.is_empty() {
                let sched_name = format!("gcpx-sched-export-{}", name);
                let wf_name = format!("gcpx-wf-export-{}", name);
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

    fn make_export_struct() -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string("proj")),
                ("region".to_owned(), prost_string("us-central1")),
                ("name".to_owned(), prost_string("my-export")),
                ("sourceDataset".to_owned(), prost_string("analytics")),
                ("sourceTable".to_owned(), prost_string("daily")),
                ("databaseType".to_owned(), prost_string("mssql")),
                (
                    "connectionString".to_owned(),
                    prost_string("jdbc:sqlserver://host:1433"),
                ),
                ("secret".to_owned(), prost_string("projects/p/secrets/s")),
                ("destTable".to_owned(), prost_string("test.dbo.daily")),
                ("imageUri".to_owned(), prost_string("gcr.io/img")),
                (
                    "scriptUri".to_owned(),
                    prost_string("gs://bucket/export.py"),
                ),
                ("serviceAccount".to_owned(), prost_string("sa@iam")),
                ("stagingBucket".to_owned(), prost_string("staging")),
                ("processBucket".to_owned(), prost_string("process")),
                ("subnetworkUri".to_owned(), prost_string("subnet")),
                ("schedule".to_owned(), prost_string("0 4 * * *")),
                ("timeZone".to_owned(), prost_string("UTC")),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn test_check_export_valid() {
        let s = make_export_struct();
        let inputs = parse_export_inputs(&s).unwrap();
        let failures = validate_export(&inputs);
        assert!(failures.is_empty());
    }

    #[test]
    fn test_check_export_jdbc_prefix_mismatch() {
        let mut s = make_export_struct();
        s.fields.insert(
            "connectionString".to_owned(),
            prost_string("jdbc:oracle:thin:@host"),
        );
        let inputs = parse_export_inputs(&s).unwrap();
        let failures = validate_export(&inputs);
        assert!(failures.iter().any(|f| f.property == "connectionString"));
    }

    #[test]
    fn test_check_export_missing_source_dataset() {
        let mut s = make_export_struct();
        s.fields
            .insert("sourceDataset".to_owned(), prost_string(""));
        let inputs = parse_export_inputs(&s).unwrap();
        let failures = validate_export(&inputs);
        assert!(failures.iter().any(|f| f.property == "sourceDataset"));
    }

    #[test]
    fn test_check_export_missing_dest_table() {
        let mut s = make_export_struct();
        s.fields.insert("destTable".to_owned(), prost_string(""));
        let inputs = parse_export_inputs(&s).unwrap();
        let failures = validate_export(&inputs);
        assert!(failures.iter().any(|f| f.property == "destTable"));
    }

    #[tokio::test]
    async fn test_diff_export_no_changes() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = make_export_struct();
        let resp = p
            .diff_export_job(pulumirpc::DiffRequest {
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
    async fn test_diff_export_source_dataset_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = make_export_struct();
        let mut news = make_export_struct();
        news.fields
            .insert("sourceDataset".to_owned(), prost_string("other"));
        let resp = p
            .diff_export_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp
            .into_inner()
            .replaces
            .contains(&"sourceDataset".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_export_dest_table_updates() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = make_export_struct();
        let mut news = make_export_struct();
        news.fields
            .insert("destTable".to_owned(), prost_string("other.dbo.t"));
        let resp = p
            .diff_export_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
        assert!(inner.replaces.is_empty());
    }

    #[tokio::test]
    async fn test_create_export_preview() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = make_export_struct();
        let resp = p
            .create_export_job(pulumirpc::CreateRequest {
                properties: Some(s),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "export-job/proj/us-central1/my-export");
        assert!(p.client.workflow_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_export_calls_apis() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = make_export_struct();
        let resp = p
            .create_export_job(pulumirpc::CreateRequest {
                properties: Some(s),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().properties.is_some());
        assert!(!p.client.workflow_log.lock().unwrap().is_empty());
        assert!(!p.client.scheduler_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_delete_export_cleans_up() {
        let p = GcpxProvider::new(MockGcpClient {
            fail_on: std::sync::Mutex::new(Some("get_scheduler_job".to_owned())),
            ..MockGcpClient::new(vec![])
        });
        let props = make_export_struct();
        let result = p
            .delete_export_job(pulumirpc::DeleteRequest {
                id: "export-job/proj/us-central1/my-export".into(),
                properties: Some(props),
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
    }
}
