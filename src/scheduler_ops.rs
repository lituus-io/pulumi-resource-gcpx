use std::future::Future;

/// Metadata returned from GCP Workflows API.
pub struct WorkflowMeta {
    pub name: String,
    pub state: String,
    pub revision_id: String,
    pub create_time: String,
    pub update_time: String,
    pub service_account: String,
}

/// Metadata returned from Cloud Scheduler API.
pub struct SchedulerJobMeta {
    pub name: String,
    pub state: String,
    pub schedule: String,
    pub time_zone: String,
    pub schedule_time: String,
    pub last_attempt_time: String,
    pub user_update_time: String,
}

/// Trait for GCP Workflows + Cloud Scheduler operations.
///
/// Uses RPITIT — no boxing. All methods use explicit `'a` lifetime.
pub trait SchedulerOps: Send + Sync + 'static {
    type Error: crate::gcp_client::GcpApiError;

    fn create_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
        sa: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn get_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn update_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn delete_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn create_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn get_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn patch_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn pause_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn resume_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn delete_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
