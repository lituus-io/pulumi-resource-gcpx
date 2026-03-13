use crate::output::OutputBuilder;
use crate::prost_util::{get_bool, get_number, get_str};
use crate::scheduler::types::{SqlJobInputs, SqlJobState};

pub fn parse_sqljob_inputs(s: &prost_types::Struct) -> Result<SqlJobInputs<'_>, &'static str> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID")?;
    let region = get_str(&s.fields, "region")
        .ok_or("missing required field 'region': provide a GCP region (e.g., 'us-central1')")?;
    let name = get_str(&s.fields, "name")
        .ok_or("missing required field 'name': provide a unique job identifier")?;
    let sql = get_str(&s.fields, "sql")
        .ok_or("missing required field 'sql': provide the BigQuery SQL to execute on schedule")?;
    let schedule = get_str(&s.fields, "schedule").ok_or("missing required field 'schedule': provide a cron expression (e.g., '0 2 * * *' for daily at 2 AM)")?;
    let time_zone = get_str(&s.fields, "timeZone").unwrap_or("UTC");
    let service_account = get_str(&s.fields, "serviceAccount").ok_or("missing required field 'serviceAccount': provide a service account email (e.g., 'sa@project.iam.gserviceaccount.com')")?;
    let description = get_str(&s.fields, "description");
    let paused = get_bool(&s.fields, "paused");
    let retry_count = get_number(&s.fields, "retryCount").map(|n| n as i32);
    let attempt_deadline = get_str(&s.fields, "attemptDeadline");

    Ok(SqlJobInputs {
        project,
        region,
        name,
        sql,
        schedule,
        time_zone,
        service_account,
        description,
        paused,
        retry_count,
        attempt_deadline,
    })
}

pub fn build_sqljob_output(inputs: &SqlJobInputs<'_>, state: &SqlJobState) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("region", inputs.region)
        .str("name", inputs.name)
        .str("sql", inputs.sql)
        .str("schedule", inputs.schedule)
        .str("timeZone", inputs.time_zone)
        .str("serviceAccount", inputs.service_account)
        .str("workflowName", &state.workflow_name)
        .str("schedulerJobName", &state.scheduler_job_name)
        .str("state", &state.state)
        .str("nextRunTime", &state.next_run_time)
        .str_opt("description", inputs.description)
        .bool_opt("paused", inputs.paused)
        .num_opt("retryCount", inputs.retry_count.map(|n| n as i64))
        .str_opt("attemptDeadline", inputs.attempt_deadline)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_util::{prost_bool, prost_number, prost_string};

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

    #[test]
    fn parse_sqljob_inputs_all_fields() {
        let mut s = make_sqljob_struct(
            "proj",
            "us-central1",
            "my-job",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        s.fields
            .insert("description".to_owned(), prost_string("test job"));
        s.fields.insert("paused".to_owned(), prost_bool(true));
        s.fields.insert("retryCount".to_owned(), prost_number(3.0));
        s.fields
            .insert("attemptDeadline".to_owned(), prost_string("300s"));

        let inputs = parse_sqljob_inputs(&s).unwrap();
        assert_eq!(inputs.project, "proj");
        assert_eq!(inputs.region, "us-central1");
        assert_eq!(inputs.name, "my-job");
        assert_eq!(inputs.sql, "SELECT 1");
        assert_eq!(inputs.schedule, "0 * * * *");
        assert_eq!(inputs.time_zone, "UTC");
        assert_eq!(inputs.service_account, "sa@iam");
        assert_eq!(inputs.description, Some("test job"));
        assert_eq!(inputs.paused, Some(true));
        assert_eq!(inputs.retry_count, Some(3));
        assert_eq!(inputs.attempt_deadline, Some("300s"));
    }

    #[test]
    fn parse_sqljob_inputs_defaults() {
        let s = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let inputs = parse_sqljob_inputs(&s).unwrap();
        assert_eq!(inputs.time_zone, "UTC");
        assert!(inputs.description.is_none());
        assert!(inputs.paused.is_none());
        assert!(inputs.retry_count.is_none());
        assert!(inputs.attempt_deadline.is_none());
    }

    #[test]
    fn parse_sqljob_inputs_missing_required() {
        // Missing project.
        let s = prost_types::Struct {
            fields: vec![
                ("region".to_owned(), prost_string("us-central1")),
                ("name".to_owned(), prost_string("j")),
                ("sql".to_owned(), prost_string("SELECT 1")),
                ("schedule".to_owned(), prost_string("0 * * * *")),
                ("serviceAccount".to_owned(), prost_string("sa@iam")),
            ]
            .into_iter()
            .collect(),
        };
        let err = parse_sqljob_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'project'"));

        // Missing sql.
        let s = prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string("proj")),
                ("region".to_owned(), prost_string("us-central1")),
                ("name".to_owned(), prost_string("j")),
                ("schedule".to_owned(), prost_string("0 * * * *")),
                ("serviceAccount".to_owned(), prost_string("sa@iam")),
            ]
            .into_iter()
            .collect(),
        };
        let err = parse_sqljob_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'sql'"));
    }

    #[test]
    fn build_sqljob_output_all_fields() {
        let inputs = SqlJobInputs {
            project: "proj",
            region: "us-central1",
            name: "my-job",
            sql: "SELECT 1",
            schedule: "0 * * * *",
            time_zone: "UTC",
            service_account: "sa@iam",
            description: Some("a job"),
            paused: Some(false),
            retry_count: Some(2),
            attempt_deadline: Some("600s"),
        };
        let state = SqlJobState {
            workflow_name: "wf-my-job".to_owned(),
            scheduler_job_name: "sched-my-job".to_owned(),
            state: "ENABLED".to_owned(),
            next_run_time: "2026-01-01T00:00:00Z".to_owned(),
        };

        let output = build_sqljob_output(&inputs, &state);
        assert_eq!(get_str(&output.fields, "project"), Some("proj"));
        assert_eq!(get_str(&output.fields, "region"), Some("us-central1"));
        assert_eq!(get_str(&output.fields, "name"), Some("my-job"));
        assert_eq!(get_str(&output.fields, "sql"), Some("SELECT 1"));
        assert_eq!(get_str(&output.fields, "schedule"), Some("0 * * * *"));
        assert_eq!(get_str(&output.fields, "workflowName"), Some("wf-my-job"));
        assert_eq!(
            get_str(&output.fields, "schedulerJobName"),
            Some("sched-my-job")
        );
        assert_eq!(get_str(&output.fields, "state"), Some("ENABLED"));
        assert_eq!(get_str(&output.fields, "description"), Some("a job"));
        assert_eq!(get_bool(&output.fields, "paused"), Some(false));
    }
}
