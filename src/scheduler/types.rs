#[derive(Debug)]
pub struct SqlJobInputs<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub name: &'a str,
    pub sql: &'a str,
    pub schedule: &'a str,
    pub time_zone: &'a str,
    pub service_account: &'a str,
    pub description: Option<&'a str>,
    pub paused: Option<bool>,
    pub retry_count: Option<i32>,
    pub attempt_deadline: Option<&'a str>,
}

pub struct SqlJobState {
    pub workflow_name: String,
    pub scheduler_job_name: String,
    pub state: String,
    pub next_run_time: String,
}
