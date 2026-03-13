use crate::dataproc::types::{
    DatabaseType, DataprocJobState, DestinationType, ExportJobInputs, IngestJobInputs,
};
use crate::output::OutputBuilder;
use crate::prost_util::{get_bool, get_number, get_str, get_string_list};

pub fn parse_ingest_inputs(s: &prost_types::Struct) -> Result<IngestJobInputs<'_>, String> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID")?;
    let region = get_str(&s.fields, "region")
        .ok_or("missing required field 'region': provide a GCP region (e.g., 'us-central1')")?;
    let name = get_str(&s.fields, "name")
        .ok_or("missing required field 'name': provide a unique job identifier")?;
    let db_type_str = get_str(&s.fields, "databaseType").ok_or(
        "missing required field 'databaseType': provide one of oracle, mssql, mysql, postgres",
    )?;
    let database_type = DatabaseType::parse(db_type_str).ok_or_else(|| {
        format!(
            "invalid databaseType '{db_type_str}': must be one of oracle, mssql, mysql, postgres"
        )
    })?;
    let connection_string = get_str(&s.fields, "connectionString")
        .ok_or("missing required field 'connectionString': provide a JDBC URI")?;
    let secret = get_str(&s.fields, "secret")
        .ok_or("missing required field 'secret': provide a Secret Manager secret name")?;
    let query = get_str(&s.fields, "query")
        .ok_or("missing required field 'query': provide the SQL pushdown query")?;
    let dest_str = get_str(&s.fields, "destination")
        .ok_or("missing required field 'destination': provide 'gcs' or 'bigquery'")?;
    let destination = DestinationType::parse(dest_str)
        .ok_or_else(|| format!("invalid destination '{dest_str}': must be 'gcs' or 'bigquery'"))?;
    let landing_bucket = get_str(&s.fields, "landingBucket");
    let dest_project = get_str(&s.fields, "destProject");
    let dest_dataset = get_str(&s.fields, "destDataset");
    let dest_table = get_str(&s.fields, "destTable");
    let image_uri = get_str(&s.fields, "imageUri")
        .ok_or("missing required field 'imageUri': provide the container image URI")?;
    let script_uri = get_str(&s.fields, "scriptUri")
        .ok_or("missing required field 'scriptUri': provide the gs:// path to main.py")?;
    let jar_uris = get_string_list(&s.fields, "jarUris");
    let runtime_version = get_str(&s.fields, "runtimeVersion").unwrap_or("2.2");
    let service_account = get_str(&s.fields, "serviceAccount")
        .ok_or("missing required field 'serviceAccount': provide a service account email")?;
    let staging_bucket = get_str(&s.fields, "stagingBucket")
        .ok_or("missing required field 'stagingBucket': provide a GCS bucket for staging")?;
    let process_bucket = get_str(&s.fields, "processBucket")
        .ok_or("missing required field 'processBucket': provide a GCS bucket for processing")?;
    let subnetwork_uri = get_str(&s.fields, "subnetworkUri")
        .ok_or("missing required field 'subnetworkUri': provide the VPC subnetwork URI")?;
    let network_tags = get_string_list(&s.fields, "networkTags");
    let network_tags = if network_tags.is_empty() {
        vec!["spark"]
    } else {
        network_tags
    };
    let schedule = get_str(&s.fields, "schedule")
        .ok_or("missing required field 'schedule': provide a cron expression")?;
    let time_zone = get_str(&s.fields, "timeZone").unwrap_or("UTC");
    let paused = get_bool(&s.fields, "paused");
    let description = get_str(&s.fields, "description");
    let retry_count = get_number(&s.fields, "retryCount").map(|n| n as i32);
    let attempt_deadline = get_str(&s.fields, "attemptDeadline");

    Ok(IngestJobInputs {
        project,
        region,
        name,
        database_type,
        connection_string,
        secret,
        query,
        destination,
        landing_bucket,
        dest_project,
        dest_dataset,
        dest_table,
        image_uri,
        script_uri,
        jar_uris,
        runtime_version,
        service_account,
        staging_bucket,
        process_bucket,
        subnetwork_uri,
        network_tags,
        schedule,
        time_zone,
        paused,
        description,
        retry_count,
        attempt_deadline,
    })
}

pub fn parse_export_inputs(s: &prost_types::Struct) -> Result<ExportJobInputs<'_>, String> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID")?;
    let region = get_str(&s.fields, "region")
        .ok_or("missing required field 'region': provide a GCP region")?;
    let name = get_str(&s.fields, "name")
        .ok_or("missing required field 'name': provide a unique job identifier")?;
    let source_dataset = get_str(&s.fields, "sourceDataset")
        .ok_or("missing required field 'sourceDataset': provide the BigQuery source dataset")?;
    let source_table = get_str(&s.fields, "sourceTable")
        .ok_or("missing required field 'sourceTable': provide the BigQuery source table")?;
    let db_type_str = get_str(&s.fields, "databaseType").ok_or(
        "missing required field 'databaseType': provide one of oracle, mssql, mysql, postgres",
    )?;
    let database_type = DatabaseType::parse(db_type_str).ok_or_else(|| {
        format!(
            "invalid databaseType '{db_type_str}': must be one of oracle, mssql, mysql, postgres"
        )
    })?;
    let connection_string = get_str(&s.fields, "connectionString")
        .ok_or("missing required field 'connectionString': provide a JDBC URI")?;
    let secret = get_str(&s.fields, "secret")
        .ok_or("missing required field 'secret': provide a Secret Manager secret name")?;
    let dest_table = get_str(&s.fields, "destTable")
        .ok_or("missing required field 'destTable': provide the on-prem destination table")?;
    let image_uri = get_str(&s.fields, "imageUri")
        .ok_or("missing required field 'imageUri': provide the container image URI")?;
    let script_uri = get_str(&s.fields, "scriptUri")
        .ok_or("missing required field 'scriptUri': provide the gs:// path to main.py")?;
    let jar_uris = get_string_list(&s.fields, "jarUris");
    let runtime_version = get_str(&s.fields, "runtimeVersion").unwrap_or("2.2");
    let service_account = get_str(&s.fields, "serviceAccount")
        .ok_or("missing required field 'serviceAccount': provide a service account email")?;
    let staging_bucket = get_str(&s.fields, "stagingBucket")
        .ok_or("missing required field 'stagingBucket': provide a GCS bucket for staging")?;
    let process_bucket = get_str(&s.fields, "processBucket")
        .ok_or("missing required field 'processBucket': provide a GCS bucket for processing")?;
    let subnetwork_uri = get_str(&s.fields, "subnetworkUri")
        .ok_or("missing required field 'subnetworkUri': provide the VPC subnetwork URI")?;
    let network_tags = get_string_list(&s.fields, "networkTags");
    let network_tags = if network_tags.is_empty() {
        vec!["spark"]
    } else {
        network_tags
    };
    let schedule = get_str(&s.fields, "schedule")
        .ok_or("missing required field 'schedule': provide a cron expression")?;
    let time_zone = get_str(&s.fields, "timeZone").unwrap_or("UTC");
    let paused = get_bool(&s.fields, "paused");
    let description = get_str(&s.fields, "description");
    let retry_count = get_number(&s.fields, "retryCount").map(|n| n as i32);
    let attempt_deadline = get_str(&s.fields, "attemptDeadline");

    Ok(ExportJobInputs {
        project,
        region,
        name,
        source_dataset,
        source_table,
        database_type,
        connection_string,
        secret,
        dest_table,
        image_uri,
        script_uri,
        jar_uris,
        runtime_version,
        service_account,
        staging_bucket,
        process_bucket,
        subnetwork_uri,
        network_tags,
        schedule,
        time_zone,
        paused,
        description,
        retry_count,
        attempt_deadline,
    })
}

pub fn build_ingest_output(
    inputs: &IngestJobInputs<'_>,
    state: &DataprocJobState,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("region", inputs.region)
        .str("name", inputs.name)
        .str("databaseType", inputs.database_type.label())
        .str("connectionString", inputs.connection_string)
        .str("secret", inputs.secret)
        .str("query", inputs.query)
        .str("destination", inputs.destination.label())
        .str_opt("landingBucket", inputs.landing_bucket)
        .str_opt("destProject", inputs.dest_project)
        .str_opt("destDataset", inputs.dest_dataset)
        .str_opt("destTable", inputs.dest_table)
        .str("imageUri", inputs.image_uri)
        .str("scriptUri", inputs.script_uri)
        .str_list("jarUris", &inputs.jar_uris)
        .str("runtimeVersion", inputs.runtime_version)
        .str("serviceAccount", inputs.service_account)
        .str("stagingBucket", inputs.staging_bucket)
        .str("processBucket", inputs.process_bucket)
        .str("subnetworkUri", inputs.subnetwork_uri)
        .str_list("networkTags", &inputs.network_tags)
        .str("schedule", inputs.schedule)
        .str("timeZone", inputs.time_zone)
        .bool_opt("paused", inputs.paused)
        .str_opt("description", inputs.description)
        .num_opt("retryCount", inputs.retry_count.map(|n| n as i64))
        .str_opt("attemptDeadline", inputs.attempt_deadline)
        .str("workflowName", &state.workflow_name)
        .str("schedulerJobName", &state.scheduler_job_name)
        .str("state", &state.state)
        .str("nextRunTime", &state.next_run_time)
        .build()
}

pub fn build_export_output(
    inputs: &ExportJobInputs<'_>,
    state: &DataprocJobState,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("region", inputs.region)
        .str("name", inputs.name)
        .str("sourceDataset", inputs.source_dataset)
        .str("sourceTable", inputs.source_table)
        .str("databaseType", inputs.database_type.label())
        .str("connectionString", inputs.connection_string)
        .str("secret", inputs.secret)
        .str("destTable", inputs.dest_table)
        .str("imageUri", inputs.image_uri)
        .str("scriptUri", inputs.script_uri)
        .str_list("jarUris", &inputs.jar_uris)
        .str("runtimeVersion", inputs.runtime_version)
        .str("serviceAccount", inputs.service_account)
        .str("stagingBucket", inputs.staging_bucket)
        .str("processBucket", inputs.process_bucket)
        .str("subnetworkUri", inputs.subnetwork_uri)
        .str_list("networkTags", &inputs.network_tags)
        .str("schedule", inputs.schedule)
        .str("timeZone", inputs.time_zone)
        .bool_opt("paused", inputs.paused)
        .str_opt("description", inputs.description)
        .num_opt("retryCount", inputs.retry_count.map(|n| n as i64))
        .str_opt("attemptDeadline", inputs.attempt_deadline)
        .str("workflowName", &state.workflow_name)
        .str("schedulerJobName", &state.scheduler_job_name)
        .str("state", &state.state)
        .str("nextRunTime", &state.next_run_time)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_util::{get_str, prost_list, prost_string};

    fn make_ingest_struct() -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string("proj")),
                ("region".to_owned(), prost_string("us-central1")),
                ("name".to_owned(), prost_string("my-ingest")),
                ("databaseType".to_owned(), prost_string("oracle")),
                (
                    "connectionString".to_owned(),
                    prost_string("jdbc:oracle:thin:@host:1521:db"),
                ),
                (
                    "secret".to_owned(),
                    prost_string("projects/proj/secrets/db-pw"),
                ),
                ("query".to_owned(), prost_string("SELECT * FROM users")),
                ("destination".to_owned(), prost_string("gcs")),
                ("landingBucket".to_owned(), prost_string("my-landing")),
                (
                    "imageUri".to_owned(),
                    prost_string("gcr.io/proj/spark:latest"),
                ),
                ("scriptUri".to_owned(), prost_string("gs://bucket/main.py")),
                (
                    "jarUris".to_owned(),
                    prost_list(vec![prost_string("gs://jars/ojdbc8.jar")]),
                ),
                (
                    "serviceAccount".to_owned(),
                    prost_string("sa@proj.iam.gserviceaccount.com"),
                ),
                ("stagingBucket".to_owned(), prost_string("my-staging")),
                ("processBucket".to_owned(), prost_string("my-process")),
                (
                    "subnetworkUri".to_owned(),
                    prost_string("projects/proj/regions/us-central1/subnetworks/default"),
                ),
                ("schedule".to_owned(), prost_string("0 2 * * *")),
                ("timeZone".to_owned(), prost_string("America/New_York")),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn make_export_struct() -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string("proj")),
                ("region".to_owned(), prost_string("us-central1")),
                ("name".to_owned(), prost_string("my-export")),
                ("sourceDataset".to_owned(), prost_string("analytics")),
                ("sourceTable".to_owned(), prost_string("daily_report")),
                ("databaseType".to_owned(), prost_string("mssql")),
                (
                    "connectionString".to_owned(),
                    prost_string("jdbc:sqlserver://host:1433;databaseName=db"),
                ),
                (
                    "secret".to_owned(),
                    prost_string("projects/proj/secrets/db-pw"),
                ),
                (
                    "destTable".to_owned(),
                    prost_string("test.dbo.daily_report"),
                ),
                (
                    "imageUri".to_owned(),
                    prost_string("gcr.io/proj/spark:latest"),
                ),
                (
                    "scriptUri".to_owned(),
                    prost_string("gs://bucket/export.py"),
                ),
                (
                    "serviceAccount".to_owned(),
                    prost_string("sa@proj.iam.gserviceaccount.com"),
                ),
                ("stagingBucket".to_owned(), prost_string("my-staging")),
                ("processBucket".to_owned(), prost_string("my-process")),
                (
                    "subnetworkUri".to_owned(),
                    prost_string("projects/proj/regions/us-central1/subnetworks/default"),
                ),
                ("schedule".to_owned(), prost_string("0 4 * * *")),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn test_parse_ingest_all_fields() {
        let s = make_ingest_struct();
        let inputs = parse_ingest_inputs(&s).unwrap();
        assert_eq!(inputs.project, "proj");
        assert_eq!(inputs.region, "us-central1");
        assert_eq!(inputs.name, "my-ingest");
        assert_eq!(inputs.database_type, DatabaseType::Oracle);
        assert_eq!(inputs.connection_string, "jdbc:oracle:thin:@host:1521:db");
        assert_eq!(inputs.secret, "projects/proj/secrets/db-pw");
        assert_eq!(inputs.query, "SELECT * FROM users");
        assert_eq!(inputs.destination, DestinationType::Gcs);
        assert_eq!(inputs.landing_bucket, Some("my-landing"));
        assert_eq!(inputs.image_uri, "gcr.io/proj/spark:latest");
        assert_eq!(inputs.jar_uris, vec!["gs://jars/ojdbc8.jar"]);
        assert_eq!(inputs.time_zone, "America/New_York");
    }

    #[test]
    fn test_parse_ingest_defaults() {
        let mut s = make_ingest_struct();
        s.fields.remove("timeZone");
        s.fields.remove("jarUris");
        let inputs = parse_ingest_inputs(&s).unwrap();
        assert_eq!(inputs.time_zone, "UTC");
        assert_eq!(inputs.runtime_version, "2.2");
        assert!(inputs.jar_uris.is_empty());
        assert_eq!(inputs.network_tags, vec!["spark"]);
    }

    #[test]
    fn test_parse_ingest_missing_project() {
        let mut s = make_ingest_struct();
        s.fields.remove("project");
        let err = parse_ingest_inputs(&s).unwrap_err();
        assert!(err.contains("project"));
    }

    #[test]
    fn test_parse_ingest_missing_query() {
        let mut s = make_ingest_struct();
        s.fields.remove("query");
        let err = parse_ingest_inputs(&s).unwrap_err();
        assert!(err.contains("query"));
    }

    #[test]
    fn test_parse_ingest_invalid_database_type() {
        let mut s = make_ingest_struct();
        s.fields
            .insert("databaseType".to_owned(), prost_string("sqlite"));
        let err = parse_ingest_inputs(&s).unwrap_err();
        assert!(err.contains("invalid databaseType"));
    }

    #[test]
    fn test_parse_ingest_invalid_destination() {
        let mut s = make_ingest_struct();
        s.fields
            .insert("destination".to_owned(), prost_string("s3"));
        let err = parse_ingest_inputs(&s).unwrap_err();
        assert!(err.contains("invalid destination"));
    }

    #[test]
    fn test_parse_export_all_fields() {
        let s = make_export_struct();
        let inputs = parse_export_inputs(&s).unwrap();
        assert_eq!(inputs.project, "proj");
        assert_eq!(inputs.source_dataset, "analytics");
        assert_eq!(inputs.source_table, "daily_report");
        assert_eq!(inputs.database_type, DatabaseType::Mssql);
        assert_eq!(inputs.dest_table, "test.dbo.daily_report");
    }

    #[test]
    fn test_parse_export_missing_source_dataset() {
        let mut s = make_export_struct();
        s.fields.remove("sourceDataset");
        let err = parse_export_inputs(&s).unwrap_err();
        assert!(err.contains("sourceDataset"));
    }

    #[test]
    fn test_build_ingest_output_roundtrip() {
        let s = make_ingest_struct();
        let inputs = parse_ingest_inputs(&s).unwrap();
        let state = DataprocJobState {
            workflow_name: "wf".to_owned(),
            scheduler_job_name: "sched".to_owned(),
            state: "ENABLED".to_owned(),
            next_run_time: "2026-01-01T00:00:00Z".to_owned(),
        };
        let output = build_ingest_output(&inputs, &state);
        assert_eq!(get_str(&output.fields, "project"), Some("proj"));
        assert_eq!(get_str(&output.fields, "databaseType"), Some("oracle"));
        assert_eq!(get_str(&output.fields, "destination"), Some("gcs"));
        assert_eq!(get_str(&output.fields, "workflowName"), Some("wf"));
    }

    #[test]
    fn test_build_export_output_roundtrip() {
        let s = make_export_struct();
        let inputs = parse_export_inputs(&s).unwrap();
        let state = DataprocJobState {
            workflow_name: "wf".to_owned(),
            scheduler_job_name: "sched".to_owned(),
            state: "ENABLED".to_owned(),
            next_run_time: String::new(),
        };
        let output = build_export_output(&inputs, &state);
        assert_eq!(get_str(&output.fields, "sourceDataset"), Some("analytics"));
        assert_eq!(
            get_str(&output.fields, "destTable"),
            Some("test.dbo.daily_report")
        );
    }
}
