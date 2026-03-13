//! Shared lifecycle helpers: 409 auto-adopt and verified delete.
//!
//! Eliminates duplicated create-or-adopt (5×) and delete-poll (5×) patterns
//! across handler files.

use std::future::Future;
use std::time::Duration;

use tonic::{Response, Status};

use crate::gcp_client::GcpApiError;

/// Creates a resource, auto-adopting (patching) on 409 conflict.
///
/// - `create_fut`: the initial create call future
/// - `adopt_fn`: closure that returns a future for the adopt/patch call (only called on 409)
/// - `context`: resource type name for error messages (e.g., "dataset")
pub async fn create_or_adopt<M, E, CreateFut, AdoptFn, AdoptFut>(
    create_fut: CreateFut,
    adopt_fn: AdoptFn,
    context: &str,
) -> Result<M, Status>
where
    E: GcpApiError,
    CreateFut: Future<Output = Result<M, E>>,
    AdoptFn: FnOnce() -> AdoptFut,
    AdoptFut: Future<Output = Result<M, E>>,
{
    match create_fut.await {
        Ok(m) => Ok(m),
        Err(e) if e.is_conflict() => adopt_fn()
            .await
            .map_err(|e| Status::internal(format!("409 auto-adopt {} failed: {}", context, e))),
        Err(e) => Err(Status::internal(format!(
            "failed to create {}: {}",
            context, e
        ))),
    }
}

/// Deletes a resource and polls until confirmed gone (404).
///
/// - `delete_fut`: the delete call future
/// - `poll_fn`: closure that returns a future to check if resource still exists
/// - `max_attempts`: maximum number of poll attempts
/// - `interval`: delay between poll attempts
pub async fn verified_delete<T, E, DeleteFut, PollFn, PollFut>(
    delete_fut: DeleteFut,
    poll_fn: PollFn,
    max_attempts: u32,
    interval: Duration,
) -> Result<Response<()>, Status>
where
    E: GcpApiError,
    DeleteFut: Future<Output = Result<(), E>>,
    PollFn: Fn() -> PollFut,
    PollFut: Future<Output = Result<T, E>>,
{
    delete_fut
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    for _ in 0..max_attempts {
        tokio::time::sleep(interval).await;
        match poll_fn().await {
            Err(e) if e.is_not_found() => return Ok(Response::new(())),
            _ => continue,
        }
    }

    Ok(Response::new(()))
}
