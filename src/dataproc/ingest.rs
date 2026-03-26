use std::borrow::Cow;

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::dataproc::job_lifecycle::delete_scheduler_and_workflow;
use crate::dataproc::parse::{build_ingest_output, parse_ingest_inputs};
use crate::dataproc::scheduler_body::{
    build_scheduler_create_body, build_scheduler_patch_body, SchedulerBodyConfig,
};
use crate::dataproc::types::{DataprocJobState, DestinationType, IngestJobInputs};
use crate::dataproc::workflow_template::{
    build_ingest_bq_args, build_ingest_gcs_args, generate_dataproc_workflow_yaml,
};
use crate::error::IntoStatusWith;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::prost_util::get_str;
use crate::provider::GcpxProvider;
use crate::resource::require_non_empty;
use crate::scheduler_ops::SchedulerOps;
use crate::schema::types::CheckFailure;

fn validate_ingest(inputs: &IngestJobInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "region", inputs.region);
    require_non_empty(&mut failures, "name", inputs.name);
    require_non_empty(&mut failures, "connectionString", inputs.connection_string);
    require_non_empty(&mut failures, "secret", inputs.secret);
    require_non_empty(&mut failures, "query", inputs.query);
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

    // Destination-specific validation.
    match inputs.destination {
        DestinationType::Gcs => {
            if inputs.landing_bucket.is_none() {
                failures.push(CheckFailure {
                    property: Cow::Borrowed("landingBucket"),
                    reason: Cow::Borrowed("landingBucket is required when destination is 'gcs'"),
                });
            }
            if inputs.dest_dataset.is_some() || inputs.dest_table.is_some() {
                failures.push(CheckFailure {
                    property: Cow::Borrowed("destination"),
                    reason: Cow::Borrowed("destDataset/destTable should not be set when destination is 'gcs' — use landingBucket instead"),
                });
            }
        }
        DestinationType::BigQuery => {
            if inputs.dest_dataset.is_none() {
                failures.push(CheckFailure {
                    property: Cow::Borrowed("destDataset"),
                    reason: Cow::Borrowed("destDataset is required when destination is 'bigquery'"),
                });
            }
            if inputs.dest_table.is_none() {
                failures.push(CheckFailure {
                    property: Cow::Borrowed("destTable"),
                    reason: Cow::Borrowed("destTable is required when destination is 'bigquery'"),
                });
            }
            if inputs.landing_bucket.is_some() {
                failures.push(CheckFailure {
                    property: Cow::Borrowed("destination"),
                    reason: Cow::Borrowed("landingBucket should not be set when destination is 'bigquery' — use destDataset/destTable instead"),
                });
            }
        }
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
    inputs: &'a IngestJobInputs<'a>,
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

fn generate_ingest_workflow(inputs: &IngestJobInputs<'_>) -> String {
    let args: Vec<&str> = match inputs.destination {
        DestinationType::Gcs => build_ingest_gcs_args(
            inputs.secret,
            inputs.landing_bucket.unwrap_or(""),
            inputs.query,
            inputs.connection_string,
            inputs.process_bucket,
        ),
        DestinationType::BigQuery => build_ingest_bq_args(
            inputs.secret,
            inputs.query,
            inputs.connection_string,
            inputs.process_bucket,
            inputs.dest_project.unwrap_or(inputs.project),
            inputs.dest_dataset.unwrap_or(""),
            inputs.dest_table.unwrap_or(""),
        ),
    };

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
        inputs.destination == DestinationType::BigQuery,
    )
}

impl<C: SchedulerOps> GcpxProvider<C> {
    pub async fn check_ingest_job(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_ingest_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_ingest(&inputs);
        build_check_response(req.news, failures)
    }

    pub async fn diff_ingest_job(
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

        let old_inputs = parse_ingest_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_ingest_inputs(news).map_err(Status::invalid_argument)?;

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
        if old_inputs.destination != new_inputs.destination {
            replace_keys.push("destination");
        }

        // Update triggers.
        if old_inputs.query != new_inputs.query {
            update_keys.push("query");
        }
        if old_inputs.connection_string != new_inputs.connection_string {
            update_keys.push("connectionString");
        }
        if old_inputs.secret != new_inputs.secret {
            update_keys.push("secret");
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
        if old_inputs.landing_bucket != new_inputs.landing_bucket {
            update_keys.push("landingBucket");
        }
        if old_inputs.dest_project != new_inputs.dest_project {
            update_keys.push("destProject");
        }
        if old_inputs.dest_dataset != new_inputs.dest_dataset {
            update_keys.push("destDataset");
        }
        if old_inputs.dest_table != new_inputs.dest_table {
            update_keys.push("destTable");
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

    pub async fn create_ingest_job(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_ingest_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!(
            "ingest-job/{}/{}/{}",
            inputs.project, inputs.region, inputs.name
        );
        let wf_name = format!("gcpx-wf-ingest-{}", inputs.name);
        let sched_name = format!("gcpx-sched-ingest-{}", inputs.name);

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
            let outputs = build_ingest_output(&inputs, &state);
            return Ok(Response::new(pulumirpc::CreateResponse {
                id,
                properties: Some(outputs),
                ..Default::default()
            }));
        }

        // 1. Generate workflow YAML.
        let workflow_yaml = generate_ingest_workflow(&inputs);

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
        let sched_cfg = sched_config(&inputs, &sched_name, &wf_name);
        let sched_body = build_scheduler_create_body(&sched_cfg);

        // 4. Create Cloud Scheduler job (with rollback on failure).
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

        // 5. If paused=true, pause the scheduler (Cloud Scheduler ignores state in create body).
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
        let outputs = build_ingest_output(&inputs, &state);
        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_ingest_job(
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

    pub async fn update_ingest_job(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_ingest_inputs(news).map_err(Status::invalid_argument)?;
        let wf_name = format!("gcpx-wf-ingest-{}", inputs.name);
        let sched_name = format!("gcpx-sched-ingest-{}", inputs.name);

        if !req.preview {
            // Regenerate workflow YAML and update.
            let workflow_yaml = generate_ingest_workflow(&inputs);
            self.client
                .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
                .await
                .status_internal_with("failed to update workflow")?;

            // Patch scheduler job (state handled via dedicated pause API below).
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
        let outputs = build_ingest_output(&inputs, &state);
        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_ingest_job(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        if let Some(ref props) = req.properties {
            let project = get_str(&props.fields, "project").unwrap_or("");
            let region = get_str(&props.fields, "region").unwrap_or("");
            let name = get_str(&props.fields, "name").unwrap_or("");

            if !project.is_empty() && !region.is_empty() && !name.is_empty() {
                let sched_name = format!("gcpx-sched-ingest-{}", name);
                let wf_name = format!("gcpx-wf-ingest-{}", name);
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

    fn make_ingest_struct(destination: &str, db_type: &str, jdbc: &str) -> prost_types::Struct {
        let mut fields = vec![
            ("project".to_owned(), prost_string("proj")),
            ("region".to_owned(), prost_string("us-central1")),
            ("name".to_owned(), prost_string("my-ingest")),
            ("databaseType".to_owned(), prost_string(db_type)),
            ("connectionString".to_owned(), prost_string(jdbc)),
            ("secret".to_owned(), prost_string("projects/p/secrets/s")),
            ("query".to_owned(), prost_string("SELECT * FROM t")),
            ("destination".to_owned(), prost_string(destination)),
            ("imageUri".to_owned(), prost_string("gcr.io/img")),
            ("scriptUri".to_owned(), prost_string("gs://bucket/main.py")),
            ("serviceAccount".to_owned(), prost_string("sa@iam")),
            ("stagingBucket".to_owned(), prost_string("staging")),
            ("processBucket".to_owned(), prost_string("process")),
            ("subnetworkUri".to_owned(), prost_string("subnet")),
            ("schedule".to_owned(), prost_string("0 2 * * *")),
            ("timeZone".to_owned(), prost_string("UTC")),
        ];
        if destination == "gcs" {
            fields.push(("landingBucket".to_owned(), prost_string("landing")));
        } else {
            fields.push(("destProject".to_owned(), prost_string("proj")));
            fields.push(("destDataset".to_owned(), prost_string("ds")));
            fields.push(("destTable".to_owned(), prost_string("tbl")));
        }
        prost_types::Struct {
            fields: fields.into_iter().collect(),
        }
    }

    fn gcs_oracle_struct() -> prost_types::Struct {
        make_ingest_struct("gcs", "oracle", "jdbc:oracle:thin:@host:1521:db")
    }

    fn bq_oracle_struct() -> prost_types::Struct {
        make_ingest_struct("bigquery", "oracle", "jdbc:oracle:thin:@host:1521:db")
    }

    #[test]
    fn test_check_ingest_valid() {
        let s = gcs_oracle_struct();
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(
            failures.is_empty(),
            "unexpected failures: {:?}",
            failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_check_ingest_empty_project() {
        let mut s = gcs_oracle_struct();
        s.fields.insert("project".to_owned(), prost_string(""));
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "project"));
    }

    #[test]
    fn test_check_ingest_jdbc_prefix_mismatch_oracle() {
        let s = make_ingest_struct("gcs", "oracle", "jdbc:sqlserver://host");
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "connectionString"));
    }

    #[test]
    fn test_check_ingest_jdbc_prefix_mismatch_mssql() {
        let s = make_ingest_struct("gcs", "mssql", "jdbc:oracle:thin:@host");
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "connectionString"));
    }

    #[test]
    fn test_check_ingest_gcs_missing_landing_bucket() {
        let mut s = gcs_oracle_struct();
        s.fields.remove("landingBucket");
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "landingBucket"));
    }

    #[test]
    fn test_check_ingest_gcs_with_dest_dataset() {
        let mut s = gcs_oracle_struct();
        s.fields
            .insert("destDataset".to_owned(), prost_string("ds"));
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "destination"));
    }

    #[test]
    fn test_check_ingest_bq_missing_dest_dataset() {
        let mut s = bq_oracle_struct();
        s.fields.remove("destDataset");
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "destDataset"));
    }

    #[test]
    fn test_check_ingest_bq_with_landing_bucket() {
        let mut s = bq_oracle_struct();
        s.fields
            .insert("landingBucket".to_owned(), prost_string("bucket"));
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "destination"));
    }

    #[test]
    fn test_check_ingest_retry_count_out_of_range() {
        let mut s = gcs_oracle_struct();
        s.fields.insert(
            "retryCount".to_owned(),
            crate::prost_util::prost_number(6.0),
        );
        let inputs = parse_ingest_inputs(&s).unwrap();
        let failures = validate_ingest(&inputs);
        assert!(failures.iter().any(|f| f.property == "retryCount"));
    }

    #[tokio::test]
    async fn test_diff_ingest_no_changes() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = gcs_oracle_struct();
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
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
    async fn test_diff_ingest_project_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("project".to_owned(), prost_string("other"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.replaces.contains(&"project".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_region_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("region".to_owned(), prost_string("europe-west1"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().replaces.contains(&"region".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_name_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields.insert("name".to_owned(), prost_string("other"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().replaces.contains(&"name".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_sa_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("serviceAccount".to_owned(), prost_string("other@iam"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp
            .into_inner()
            .replaces
            .contains(&"serviceAccount".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_destination_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let news = bq_oracle_struct();
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp
            .into_inner()
            .replaces
            .contains(&"destination".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_database_type_replaces() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let news = make_ingest_struct("gcs", "mssql", "jdbc:sqlserver://host");
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp
            .into_inner()
            .replaces
            .contains(&"databaseType".to_owned()));
    }

    #[tokio::test]
    async fn test_diff_ingest_query_updates() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("query".to_owned(), prost_string("SELECT 1"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
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
    async fn test_diff_ingest_schedule_updates() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("schedule".to_owned(), prost_string("0 3 * * *"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
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
    async fn test_diff_ingest_image_uri_updates() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("imageUri".to_owned(), prost_string("gcr.io/other"));
        let resp = p
            .diff_ingest_job(pulumirpc::DiffRequest {
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
    async fn test_create_ingest_preview() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = gcs_oracle_struct();
        let resp = p
            .create_ingest_job(pulumirpc::CreateRequest {
                urn: "urn:pulumi:stack::project::gcpx:dataproc/ingestJob:IngestJob::test".into(),
                properties: Some(s),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "ingest-job/proj/us-central1/my-ingest");
        let props = inner.properties.unwrap();
        assert_eq!(get_str(&props.fields, "state"), Some("PREVIEW"));
        // No API calls in preview.
        assert!(p.client.workflow_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_ingest_gcs_calls_apis() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = gcs_oracle_struct();
        let resp = p
            .create_ingest_job(pulumirpc::CreateRequest {
                urn: "urn:pulumi:stack::project::gcpx:dataproc/ingestJob:IngestJob::test".into(),
                properties: Some(s),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "ingest-job/proj/us-central1/my-ingest");
        // Workflow and scheduler should have been created.
        assert!(!p.client.workflow_log.lock().unwrap().is_empty());
        assert!(!p.client.scheduler_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_ingest_bq_calls_apis() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let s = bq_oracle_struct();
        let resp = p
            .create_ingest_job(pulumirpc::CreateRequest {
                urn: "urn:pulumi:stack::project::gcpx:dataproc/ingestJob:IngestJob::test".into(),
                properties: Some(s),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().properties.is_some());
        // Verify workflow contains spark-bigquery JAR.
        let wf_log = p.client.workflow_log.lock().unwrap();
        assert!(!wf_log.is_empty());
        assert!(wf_log[0].2.contains("spark-bigquery"));
    }

    #[tokio::test]
    async fn test_create_ingest_scheduler_failure_rolls_back_workflow() {
        let p = GcpxProvider::new(MockGcpClient {
            fail_on: std::sync::Mutex::new(Some("create_scheduler_job".to_owned())),
            ..MockGcpClient::new(vec![])
        });
        let s = gcs_oracle_struct();
        let result = p
            .create_ingest_job(pulumirpc::CreateRequest {
                properties: Some(s),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
        // Workflow should have been rolled back (deleted).
        let wf_log = p.client.workflow_log.lock().unwrap();
        assert!(wf_log.iter().any(|(op, _, _)| op == "delete"));
    }

    #[tokio::test]
    async fn test_update_ingest_regenerates_workflow() {
        let p = GcpxProvider::new(MockGcpClient::new(vec![]));
        let olds = gcs_oracle_struct();
        let mut news = gcs_oracle_struct();
        news.fields
            .insert("query".to_owned(), prost_string("SELECT 2"));
        let result = p
            .update_ingest_job(pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let wf_log = p.client.workflow_log.lock().unwrap();
        assert!(wf_log.iter().any(|(op, _, _)| op == "update"));
    }

    #[tokio::test]
    async fn test_delete_ingest_cleans_up() {
        let p = GcpxProvider::new(MockGcpClient {
            fail_on: std::sync::Mutex::new(Some("get_scheduler_job".to_owned())),
            ..MockGcpClient::new(vec![])
        });
        let props = gcs_oracle_struct();
        let result = p
            .delete_ingest_job(pulumirpc::DeleteRequest {
                id: "ingest-job/proj/us-central1/my-ingest".into(),
                properties: Some(props),
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let sched_log = p.client.scheduler_log.lock().unwrap();
        assert!(sched_log.iter().any(|(op, _)| op == "delete"));
    }
}
