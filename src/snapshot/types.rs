#[derive(Debug)]
pub struct SnapshotInputs<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub dataset: &'a str,
    pub name: &'a str,
    pub source_sql: &'a str,
    pub unique_key: &'a str,
    pub strategy: &'a str,
    pub updated_at: &'a str,
    pub schedule: &'a str,
    pub time_zone: &'a str,
    pub service_account: &'a str,
    pub description: Option<&'a str>,
    pub paused: Option<bool>,
    pub invalidate_hard_deletes: Option<bool>,
    pub auto_optimize: Option<bool>,
    pub source_schema: Option<&'a str>,
}

pub struct SnapshotState {
    pub workflow_name: String,
    pub scheduler_job_name: String,
    pub snapshot_table: String,
    pub state: String,
    pub next_run_time: String,
}
