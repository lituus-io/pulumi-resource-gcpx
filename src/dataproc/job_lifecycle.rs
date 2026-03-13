//! Shared lifecycle helpers for workflow+scheduler resource pairs.

use tonic::{Response, Status};

use crate::gcp_client::GcpApiError;
use crate::scheduler_ops::SchedulerOps;

/// Concurrently deletes a scheduler job and workflow, then polls until both are gone.
pub async fn delete_scheduler_and_workflow<C: SchedulerOps>(
    client: &C,
    project: &str,
    region: &str,
    sched_name: &str,
    wf_name: &str,
) -> Result<Response<()>, Status> {
    let (_, _) = tokio::join!(
        client.delete_scheduler_job(project, region, sched_name),
        client.delete_workflow(project, region, wf_name),
    );

    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let (sched_result, wf_result) = tokio::join!(
            client.get_scheduler_job(project, region, sched_name),
            client.get_workflow(project, region, wf_name),
        );
        let sched_gone = sched_result.is_err_and(|e| e.is_not_found());
        let wf_gone = wf_result.is_err_and(|e| e.is_not_found());
        if sched_gone && wf_gone {
            break;
        }
    }

    Ok(Response::new(()))
}
