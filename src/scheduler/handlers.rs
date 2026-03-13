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
use crate::scheduler::parse::{build_sqljob_output, parse_sqljob_inputs};
use crate::scheduler::types::SqlJobState;
use crate::scheduler::workflow_template::generate_workflow_yaml;
use crate::scheduler_ops::SchedulerOps;
use crate::schema::types::CheckFailure;

fn validate_sqljob(inputs: &crate::scheduler::types::SqlJobInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "region", inputs.region);
    require_non_empty(&mut failures, "name", inputs.name);
    require_non_empty(&mut failures, "sql", inputs.sql);
    require_non_empty(&mut failures, "schedule", inputs.schedule);
    require_non_empty(&mut failures, "serviceAccount", inputs.service_account);

    if let Some(rc) = inputs.retry_count {
        if !(0..=5).contains(&rc) {
            failures.push(CheckFailure {
                property: "retryCount".into(),
                reason: "retryCount must be 0-5 (inclusive): use 0 for no retries, or 1-5 for automatic retry".into(),
            });
        }
    }

    failures
}

impl<C: BqOps + SchedulerOps> GcpxProvider<C> {
    pub async fn check_sql_job(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_sqljob(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_sql_job(
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

        let old_inputs = parse_sqljob_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;

        let mut replace_keys = Vec::new();
        let mut update_keys = Vec::new();

        diff_fields!(old_inputs, new_inputs, replace_keys, update_keys;
            project => replace,
            region => replace,
            name => replace,
            service_account => replace "serviceAccount",
            sql => update,
            schedule => update,
            time_zone => update "timeZone",
            paused => update,
            description => update,
            retry_count => update "retryCount",
            attempt_deadline => update "attemptDeadline",
        );

        Ok(build_diff_response(&replace_keys, &update_keys))
    }

    pub async fn create_sql_job(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_sqljob_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!(
            "sql-job/{}/{}/{}",
            inputs.project, inputs.region, inputs.name
        );

        let wf_name = format!("gcpx-wf-{}", inputs.name);
        let sched_name = format!("gcpx-sched-{}", inputs.name);

        if req.preview {
            let state = SqlJobState {
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
            let outputs = build_sqljob_output(&inputs, &state);
            return Ok(Response::new(pulumirpc::CreateResponse {
                id,
                properties: Some(outputs),
                ..Default::default()
            }));
        }

        // Validate SQL via dry-run before creating infrastructure.
        let dry_run = self
            .client
            .dry_run_query(inputs.project, inputs.sql, None)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL validation failed: {e}")))?;
        if !dry_run.valid {
            return Err(Status::invalid_argument(format!(
                "SQL is invalid — scheduler job would fail at runtime: {}",
                dry_run.error_message.unwrap_or_default()
            )));
        }

        // 1. Generate workflow YAML.
        let workflow_yaml = generate_workflow_yaml(inputs.project, inputs.sql);

        // 2. Create GCP Workflow (with 409 auto-adopt).
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

        // 3. Build scheduler job body.
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
            retry_count: inputs.retry_count,
            attempt_deadline: inputs.attempt_deadline,
        };
        let sched_body = build_scheduler_create_body(&sched_cfg);

        // 4. Create Cloud Scheduler job (with 409 auto-adopt).
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
                // Rollback: delete the workflow.
                let _ = self
                    .client
                    .delete_workflow(inputs.project, inputs.region, &wf_name)
                    .await;
                return Err(status);
            }
        };

        // Cloud Scheduler ignores state in create body — use dedicated pause API.
        let sched_meta = if inputs.paused == Some(true) {
            self.client
                .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
                .await
                .status_internal_with("failed to pause scheduler job")?
        } else {
            sched_meta
        };

        let state = SqlJobState {
            workflow_name: wf_meta.name,
            scheduler_job_name: sched_meta.name,
            state: sched_meta.state,
            next_run_time: sched_meta.schedule_time,
        };
        let outputs = build_sqljob_output(&inputs, &state);
        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_sql_job(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        // Return stored state.
        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: req.properties,
            inputs: req.inputs,
            ..Default::default()
        }))
    }

    pub async fn update_sql_job(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;
        let wf_name = format!("gcpx-wf-{}", inputs.name);
        let sched_name = format!("gcpx-sched-{}", inputs.name);

        if !req.preview {
            // Check if SQL changed -> update workflow.
            let old_sql = req
                .olds
                .as_ref()
                .and_then(|o| get_str(&o.fields, "sql"))
                .unwrap_or("");

            if old_sql != inputs.sql {
                // Validate new SQL via dry-run before updating infrastructure.
                let dry_run = self
                    .client
                    .dry_run_query(inputs.project, inputs.sql, None)
                    .await
                    .map_err(|e| Status::invalid_argument(format!("SQL validation failed: {e}")))?;
                if !dry_run.valid {
                    return Err(Status::invalid_argument(format!(
                        "SQL is invalid — scheduler job would fail at runtime: {}",
                        dry_run.error_message.unwrap_or_default()
                    )));
                }

                let workflow_yaml = generate_workflow_yaml(inputs.project, inputs.sql);
                self.client
                    .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
                    .await
                    .status_internal_with("failed to update workflow")?;
            }

            // Patch scheduler job (state handled via dedicated pause API below).
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
                retry_count: inputs.retry_count,
                attempt_deadline: inputs.attempt_deadline,
            };
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

        let state = SqlJobState {
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
        let outputs = build_sqljob_output(&inputs, &state);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_sql_job(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        if let Some(ref props) = req.properties {
            let project = get_str(&props.fields, "project").unwrap_or("");
            let region = get_str(&props.fields, "region").unwrap_or("");
            let name = get_str(&props.fields, "name").unwrap_or("");

            if !project.is_empty() && !region.is_empty() && !name.is_empty() {
                let sched_name = format!("gcpx-sched-{}", name);
                let wf_name = format!("gcpx-wf-{}", name);
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
    use crate::bq::DryRunResult;
    use crate::gcp_client::MockGcpClient;
    use crate::prost_util::prost_string;

    fn make_sqljob_struct(
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
        schedule: &str,
        sa: &str,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string(project)),
                ("region".to_owned(), prost_string(region)),
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                ("schedule".to_owned(), prost_string(schedule)),
                ("timeZone".to_owned(), prost_string("UTC")),
                ("serviceAccount".to_owned(), prost_string(sa)),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[tokio::test]
    async fn create_sql_job_dry_run_invalid_sql_rejects() {
        let p = GcpxProvider::new(MockGcpClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("Syntax error at position 7".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..MockGcpClient::new(vec![])
        });
        let props =
            make_sqljob_struct("proj", "us-central1", "j", "SELEC 1", "0 * * * *", "sa@iam");
        let result = p
            .create_sql_job(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL is invalid"));
        assert!(err.message().contains("Syntax error"));

        // No workflow or scheduler should have been created.
        assert!(p.client.workflow_log.lock().unwrap().is_empty());
        assert!(p.client.scheduler_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_sql_job_dry_run_error_rejects() {
        let p = GcpxProvider::new(MockGcpClient {
            fail_on: std::sync::Mutex::new(Some("dry_run_query".to_owned())),
            ..MockGcpClient::new(vec![])
        });
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = p
            .create_sql_job(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL validation failed"));
    }

    #[tokio::test]
    async fn create_sql_job_preview_skips_dry_run() {
        // Even with invalid dry-run, preview should succeed.
        let p = GcpxProvider::new(MockGcpClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("invalid".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..MockGcpClient::new(vec![])
        });
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = p
            .create_sql_job(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: true,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn create_sql_job_valid_sql_succeeds() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = p
            .create_sql_job(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let inner = result.unwrap().into_inner();
        assert_eq!(inner.id, "sql-job/proj/us-central1/j");
    }

    #[tokio::test]
    async fn update_sql_job_dry_run_invalid_rejects() {
        let p = GcpxProvider::new(MockGcpClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("bad SQL".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..MockGcpClient::new(vec![])
        });
        let olds = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let news = make_sqljob_struct("proj", "us-central1", "j", "SELEC 2", "0 * * * *", "sa@iam");
        let result = p
            .update_sql_job(pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            })
            .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL is invalid"));
    }

    #[tokio::test]
    async fn update_sql_job_same_sql_skips_dry_run() {
        // Same SQL means no dry-run or workflow update.
        let p = GcpxProvider::new(MockGcpClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("would fail if called".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..MockGcpClient::new(vec![])
        });
        let inputs = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = p
            .update_sql_job(pulumirpc::UpdateRequest {
                olds: Some(inputs.clone()),
                news: Some(inputs),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
    }

    fn make_sqljob_struct_with_paused(
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
        schedule: &str,
        sa: &str,
        paused: bool,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string(project)),
                ("region".to_owned(), prost_string(region)),
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                ("schedule".to_owned(), prost_string(schedule)),
                ("timeZone".to_owned(), prost_string("UTC")),
                ("serviceAccount".to_owned(), prost_string(sa)),
                (
                    "paused".to_owned(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::BoolValue(paused)),
                    },
                ),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[tokio::test]
    async fn update_sql_job_paused_false_calls_resume() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = make_sqljob_struct_with_paused(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
            true,
        );
        let news = make_sqljob_struct_with_paused(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
            false,
        );
        let result = p
            .update_sql_job(pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let sched_log = p.client.scheduler_log.lock().unwrap().clone();
        assert!(
            sched_log.iter().any(|(op, _)| op == "resume"),
            "expected resume call in scheduler log: {:?}",
            sched_log,
        );
    }

    #[tokio::test]
    async fn update_sql_job_paused_none_skips_pause_resume() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let inputs = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = p
            .update_sql_job(pulumirpc::UpdateRequest {
                olds: Some(inputs.clone()),
                news: Some(inputs),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let sched_log = p.client.scheduler_log.lock().unwrap().clone();
        assert!(
            !sched_log
                .iter()
                .any(|(op, _)| op == "pause" || op == "resume"),
            "expected no pause/resume calls: {:?}",
            sched_log,
        );
    }
}
