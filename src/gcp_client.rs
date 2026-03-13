use crate::bq::{
    convert_bq_fields, convert_dataset_response, convert_routine_response, convert_table_response,
    BqDatasetResponse, BqDryRunResponse, BqField, BqOps, BqRoutineResponse, BqTableMeta,
    BqTableResponse, DatasetMeta, DryRunResult, RoutineMeta,
};
use crate::scheduler_ops::{SchedulerJobMeta, SchedulerOps, WorkflowMeta};
use crate::token_source::TokenSource;

use serde::de::DeserializeOwned;

/// Trait for inspecting GCP API error status codes.
/// Implemented for both `GcpError` (production) and `MockError` (tests).
pub trait GcpApiError: std::error::Error + Send + Sync + 'static {
    fn is_conflict(&self) -> bool;
    fn is_not_found(&self) -> bool;
    fn is_rate_limited(&self) -> bool;
}

#[derive(Debug, thiserror::Error)]
pub enum GcpError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Auth error: {0}")]
    Auth(#[from] gcp_auth::Error),
    #[error("GCP API error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("Workflow not ready (state: {state})")]
    WorkflowNotReady { state: String },
}

impl GcpApiError for GcpError {
    fn is_conflict(&self) -> bool {
        matches!(self, GcpError::Api { status: 409, .. })
    }
    fn is_not_found(&self) -> bool {
        matches!(self, GcpError::Api { status: 404, .. })
    }
    fn is_rate_limited(&self) -> bool {
        matches!(self, GcpError::Api { status: 429, .. })
    }
}

/// HTTP GCP client — generic over `TokenSource` to contain `dyn` at boundary.
pub struct HttpGcpClient<T: TokenSource> {
    http: reqwest::Client,
    token_source: T,
    circuit_breaker: crate::circuit_breaker::CircuitBreaker,
}

const BQ_SCOPES: &[&str] = &["https://www.googleapis.com/auth/bigquery"];
const PLATFORM_SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

impl<T: TokenSource> HttpGcpClient<T> {
    pub fn new(http: reqwest::Client, token_source: T) -> Self {
        Self {
            http,
            token_source,
            circuit_breaker: crate::circuit_breaker::CircuitBreaker::default_config(),
        }
    }

    async fn bq_token(&self) -> Result<String, GcpError> {
        Ok(self.token_source.token(BQ_SCOPES).await?)
    }

    async fn platform_token(&self) -> Result<String, GcpError> {
        Ok(self.token_source.token(PLATFORM_SCOPES).await?)
    }

    // --- Retry wrapper ---

    const RETRY_DELAYS: [std::time::Duration; 4] = [
        std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(4),
        std::time::Duration::from_secs(8),
        std::time::Duration::from_secs(16),
    ];

    fn jitter() -> std::time::Duration {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_millis()
            % 1000;
        std::time::Duration::from_millis(ms as u64)
    }

    fn is_retryable(status: u16, message: &str) -> bool {
        match status {
            429 | 500 | 502 | 503 | 504 => true,
            400 | 403 => {
                message.contains("rateLimitExceeded")
                    || message.contains("jobRateLimitExceeded")
                    || message.contains("Job exceeded rate limits")
            }
            _ => false,
        }
    }

    async fn with_retry<F, Fut, R>(&self, op: F) -> Result<R, GcpError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<R, GcpError>>,
    {
        // Circuit breaker: reject immediately if open.
        self.circuit_breaker
            .allow_request()
            .map_err(|_| GcpError::Api {
                status: 503,
                message: "circuit breaker open".into(),
            })?;

        let mut last_err = None;
        for (attempt, delay) in std::iter::once(std::time::Duration::ZERO)
            .chain(Self::RETRY_DELAYS.iter().copied())
            .enumerate()
        {
            if attempt > 0 {
                // Re-check circuit breaker between retry attempts.
                self.circuit_breaker
                    .allow_request()
                    .map_err(|_| GcpError::Api {
                        status: 503,
                        message: "circuit breaker open".into(),
                    })?;
                tokio::time::sleep(delay + Self::jitter()).await;
            }
            match op().await {
                Ok(val) => {
                    self.circuit_breaker.record_success();
                    if attempt > 0 {
                        eprintln!("gcpx: succeeded after {} attempts", attempt + 1);
                    }
                    return Ok(val);
                }
                Err(GcpError::Api {
                    status,
                    ref message,
                }) if Self::is_retryable(status, message) => {
                    self.circuit_breaker.record_failure();
                    let truncated = match message.get(..200) {
                        Some(s) => s,
                        None => message,
                    };
                    last_err = Some(GcpError::Api {
                        status,
                        message: format!(
                            "retryable error on attempt {}: {}",
                            attempt + 1,
                            truncated
                        ),
                    });
                    continue;
                }
                Err(e) => {
                    // Non-retryable errors (4xx) do NOT trip the breaker.
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap())
    }

    // --- Deduplicated HTTP helpers (all with retry) ---

    async fn http_send_json<R: DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<R, GcpError> {
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(GcpError::Api {
                status: status.as_u16(),
                message: text,
            });
        }
        Ok(resp.json().await?)
    }

    async fn get_json<R: DeserializeOwned>(&self, url: &str, token: &str) -> Result<R, GcpError> {
        self.with_retry(|| self.http_send_json(self.http.get(url).bearer_auth(token)))
            .await
    }

    async fn post_json<R: DeserializeOwned>(
        &self,
        url: &str,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        let body_bytes =
            bytes::Bytes::from(serde_json::to_vec(body).map_err(|e| GcpError::Api {
                status: 400,
                message: format!("serialization error: {}", e),
            })?);
        self.with_retry(|| {
            self.http_send_json(
                self.http
                    .post(url)
                    .bearer_auth(token)
                    .header("content-type", "application/json")
                    .body(body_bytes.clone()),
            )
        })
        .await
    }

    async fn patch_json<R: DeserializeOwned>(
        &self,
        url: &str,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        let body_bytes =
            bytes::Bytes::from(serde_json::to_vec(body).map_err(|e| GcpError::Api {
                status: 400,
                message: format!("serialization error: {}", e),
            })?);
        self.with_retry(|| {
            self.http_send_json(
                self.http
                    .patch(url)
                    .bearer_auth(token)
                    .header("content-type", "application/json")
                    .body(body_bytes.clone()),
            )
        })
        .await
    }

    async fn put_json<R: DeserializeOwned>(
        &self,
        url: &str,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        let body_bytes =
            bytes::Bytes::from(serde_json::to_vec(body).map_err(|e| GcpError::Api {
                status: 400,
                message: format!("serialization error: {}", e),
            })?);
        self.with_retry(|| {
            self.http_send_json(
                self.http
                    .put(url)
                    .bearer_auth(token)
                    .header("content-type", "application/json")
                    .body(body_bytes.clone()),
            )
        })
        .await
    }

    async fn delete_ok(&self, url: &str, token: &str) -> Result<(), GcpError> {
        self.with_retry(|| async {
            let resp = self.http.delete(url).bearer_auth(token).send().await?;
            let status = resp.status();
            if !status.is_success() && status.as_u16() != 404 {
                let text = resp.text().await.unwrap_or_default();
                return Err(GcpError::Api {
                    status: status.as_u16(),
                    message: text,
                });
            }
            Ok(())
        })
        .await
    }

    async fn get_workflow_inner(
        &self,
        project: &str,
        region: &str,
        name: &str,
    ) -> Result<WorkflowMeta, GcpError> {
        let url = format!(
            "https://workflows.googleapis.com/v1/projects/{}/locations/{}/workflows/{}",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        let wf: WorkflowResponse = self.get_json(&url, &token).await?;
        Ok(to_workflow_meta(wf))
    }
}

// --- BqOps implementation: true zero-copy with 'a lifetimes ---

/// Shorthand for URL path segment encoding.
use crate::sanitize::encode_path_segment as e;

impl<T: TokenSource> BqOps for HttpGcpClient<T> {
    type Error = GcpError;

    async fn execute_ddl<'a>(
        &'a self,
        project: &'a str,
        ddl: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
            e(project),
        );
        let mut body = serde_json::json!({
            "query": ddl,
            "useLegacySql": false,
        });
        if let Some(limit) = max_bytes_billed {
            body["maximumBytesBilled"] = serde_json::Value::String(limit.to_string());
        }
        let token = self.bq_token().await?;
        let _: serde_json::Value = self.post_json(&url, &token, &body).await?;
        Ok(())
    }

    async fn get_table_schema<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<Vec<BqField>, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            e(project),
            e(dataset),
            e(table_id),
        );
        let token = self.bq_token().await?;
        let table: BqTableResponse = self.get_json(&url, &token).await?;
        Ok(table
            .schema
            .map(|s| convert_bq_fields(s.fields))
            .unwrap_or_default())
    }

    async fn create_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<BqTableMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables",
            e(project),
            e(dataset),
        );
        let token = self.bq_token().await?;
        let table: BqTableResponse = self.post_json(&url, &token, body).await?;
        Ok(convert_table_response(table))
    }

    async fn get_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<BqTableMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            e(project),
            e(dataset),
            e(table_id),
        );
        let token = self.bq_token().await?;
        let table: BqTableResponse = self.get_json(&url, &token).await?;
        Ok(convert_table_response(table))
    }

    async fn patch_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<BqTableMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            e(project),
            e(dataset),
            e(table_id),
        );
        let token = self.bq_token().await?;
        let table: BqTableResponse = self.patch_json(&url, &token, body).await?;
        Ok(convert_table_response(table))
    }

    async fn delete_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            e(project),
            e(dataset),
            e(table_id),
        );
        let token = self.bq_token().await?;
        self.delete_ok(&url, &token).await
    }

    async fn dry_run_query<'a>(
        &'a self,
        project: &'a str,
        sql: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> Result<DryRunResult, Self::Error> {
        // Circuit breaker check (dry_run_query doesn't use with_retry).
        self.circuit_breaker
            .allow_request()
            .map_err(|_| GcpError::Api {
                status: 503,
                message: "circuit breaker open".into(),
            })?;

        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
            e(project),
        );
        let mut body = serde_json::json!({
            "query": sql,
            "useLegacySql": false,
            "dryRun": true,
        });
        if let Some(limit) = max_bytes_billed {
            body["maximumBytesBilled"] = serde_json::Value::String(limit.to_string());
        }
        let token = self.bq_token().await?;
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let text = resp.text().await.unwrap_or_default();
            // Record failure for server errors; client errors don't trip breaker.
            if status_code >= 500 || status_code == 429 {
                self.circuit_breaker.record_failure();
            }
            return Ok(DryRunResult {
                valid: false,
                error_message: Some(text),
                total_bytes_processed: 0,
                schema: Vec::new(),
            });
        }
        self.circuit_breaker.record_success();
        let dr: BqDryRunResponse = resp.json().await?;
        let bytes = dr
            .statistics
            .and_then(|s| s.total_bytes_processed.parse().ok())
            .unwrap_or(0);
        let schema = dr
            .schema
            .map(|s| convert_bq_fields(s.fields))
            .unwrap_or_default();
        Ok(DryRunResult {
            valid: true,
            error_message: None,
            total_bytes_processed: bytes,
            schema,
        })
    }

    // --- Dataset operations ---

    async fn create_dataset<'a>(
        &'a self,
        project: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<DatasetMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
            e(project),
        );
        let token = self.bq_token().await?;
        let ds: BqDatasetResponse = self.post_json(&url, &token, body).await?;
        Ok(convert_dataset_response(ds))
    }

    async fn get_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> Result<DatasetMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}",
            e(project),
            e(dataset_id),
        );
        let token = self.bq_token().await?;
        let ds: BqDatasetResponse = self.get_json(&url, &token).await?;
        Ok(convert_dataset_response(ds))
    }

    async fn patch_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<DatasetMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}",
            e(project),
            e(dataset_id),
        );
        let token = self.bq_token().await?;
        let ds: BqDatasetResponse = self.patch_json(&url, &token, body).await?;
        Ok(convert_dataset_response(ds))
    }

    async fn delete_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}?deleteContents=true",
            e(project), e(dataset_id),
        );
        let token = self.bq_token().await?;
        self.delete_ok(&url, &token).await
    }

    // --- Routine operations ---

    async fn create_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<RoutineMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/routines",
            e(project),
            e(dataset),
        );
        let token = self.bq_token().await?;
        let r: BqRoutineResponse = self.post_json(&url, &token, body).await?;
        Ok(convert_routine_response(r))
    }

    async fn get_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> Result<RoutineMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/routines/{}",
            e(project),
            e(dataset),
            e(routine_id),
        );
        let token = self.bq_token().await?;
        let r: BqRoutineResponse = self.get_json(&url, &token).await?;
        Ok(convert_routine_response(r))
    }

    async fn update_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<RoutineMeta, Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/routines/{}",
            e(project),
            e(dataset),
            e(routine_id),
        );
        let token = self.bq_token().await?;
        let r: BqRoutineResponse = self.put_json(&url, &token, body).await?;
        Ok(convert_routine_response(r))
    }

    async fn delete_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/routines/{}",
            e(project),
            e(dataset),
            e(routine_id),
        );
        let token = self.bq_token().await?;
        self.delete_ok(&url, &token).await
    }
}

// --- SchedulerOps implementation ---

use serde::Deserialize;

#[derive(Deserialize)]
struct WorkflowResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default, rename = "revisionId")]
    revision_id: String,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "updateTime")]
    update_time: String,
    #[serde(default, rename = "serviceAccount")]
    service_account: String,
}

#[derive(Deserialize)]
struct SchedulerJobResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    schedule: String,
    #[serde(default, rename = "timeZone")]
    time_zone: String,
    #[serde(default, rename = "scheduleTime")]
    schedule_time: String,
    #[serde(default, rename = "lastAttemptTime")]
    last_attempt_time: String,
    #[serde(default, rename = "userUpdateTime")]
    user_update_time: String,
}

#[derive(Deserialize)]
struct LroResponse {
    #[serde(default)]
    done: bool,
    #[serde(default)]
    error: Option<LroError>,
    #[serde(default)]
    response: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct LroError {
    #[serde(default)]
    message: String,
}

impl<T: TokenSource> SchedulerOps for HttpGcpClient<T> {
    type Error = GcpError;

    async fn create_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
        sa: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        let url =
            format!(
            "https://workflows.googleapis.com/v1/projects/{}/locations/{}/workflows?workflowId={}",
            e(project), e(region), e(name),
        );
        let body = serde_json::json!({
            "sourceContents": definition,
            "serviceAccount": sa,
        });
        let token = self.platform_token().await?;
        let lro: LroResponse = self.post_json(&url, &token, &body).await?;
        if let Some(err) = lro.error {
            return Err(GcpError::Api {
                status: 400,
                message: err.message,
            });
        }
        if lro.done {
            if let Some(resp_val) = lro.response {
                let wf: WorkflowResponse =
                    serde_json::from_value(resp_val).map_err(|e| GcpError::Api {
                        status: 500,
                        message: e.to_string(),
                    })?;
                return Ok(to_workflow_meta(wf));
            }
        }
        // Poll by getting the workflow directly.
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            match self.get_workflow_inner(project, region, name).await {
                Ok(wf) if wf.state == "ACTIVE" => return Ok(wf),
                Ok(wf) => {
                    if wf.state != "UNAVAILABLE" {
                        return Err(GcpError::WorkflowNotReady { state: wf.state });
                    }
                }
                Err(_) => continue,
            }
        }
        Err(GcpError::WorkflowNotReady {
            state: "TIMEOUT".to_owned(),
        })
    }

    async fn get_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        self.get_workflow_inner(project, region, name).await
    }

    async fn update_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        let url = format!(
            "https://workflows.googleapis.com/v1/projects/{}/locations/{}/workflows/{}?updateMask=sourceContents",
            e(project), e(region), e(name),
        );
        let body = serde_json::json!({
            "sourceContents": definition,
        });
        let token = self.platform_token().await?;
        let _: serde_json::Value = self.patch_json(&url, &token, &body).await?;
        // Wait for workflow to become ACTIVE again.
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            match self.get_workflow_inner(project, region, name).await {
                Ok(wf) if wf.state == "ACTIVE" => return Ok(wf),
                Ok(_) => continue,
                Err(_) => continue,
            }
        }
        Err(GcpError::WorkflowNotReady {
            state: "TIMEOUT".to_owned(),
        })
    }

    async fn delete_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://workflows.googleapis.com/v1/projects/{}/locations/{}/workflows/{}",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        self.delete_ok(&url, &token).await
    }

    async fn create_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs",
            e(project),
            e(region),
        );
        let token = self.platform_token().await?;
        let job: SchedulerJobResponse = self.post_json(&url, &token, body).await?;
        Ok(to_scheduler_meta(job))
    }

    async fn get_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs/{}",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        let job: SchedulerJobResponse = self.get_json(&url, &token).await?;
        Ok(to_scheduler_meta(job))
    }

    async fn patch_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs/{}",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        let job: SchedulerJobResponse = self.patch_json(&url, &token, body).await?;
        Ok(to_scheduler_meta(job))
    }

    async fn pause_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs/{}:pause",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        let empty = serde_json::json!({});
        let job: SchedulerJobResponse = self.post_json(&url, &token, &empty).await?;
        Ok(to_scheduler_meta(job))
    }

    async fn resume_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs/{}:resume",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        let empty = serde_json::json!({});
        let job: SchedulerJobResponse = self.post_json(&url, &token, &empty).await?;
        Ok(to_scheduler_meta(job))
    }

    async fn delete_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<(), Self::Error> {
        let url = format!(
            "https://cloudscheduler.googleapis.com/v1/projects/{}/locations/{}/jobs/{}",
            e(project),
            e(region),
            e(name),
        );
        let token = self.platform_token().await?;
        self.delete_ok(&url, &token).await
    }
}

fn to_workflow_meta(wf: WorkflowResponse) -> WorkflowMeta {
    WorkflowMeta {
        name: wf.name,
        state: wf.state,
        revision_id: wf.revision_id,
        create_time: wf.create_time,
        update_time: wf.update_time,
        service_account: wf.service_account,
    }
}

fn to_scheduler_meta(job: SchedulerJobResponse) -> SchedulerJobMeta {
    SchedulerJobMeta {
        name: job.name,
        state: job.state,
        schedule: job.schedule,
        time_zone: job.time_zone,
        schedule_time: job.schedule_time,
        last_attempt_time: job.last_attempt_time,
        user_update_time: job.user_update_time,
    }
}

// --- Mock client for tests ---

#[cfg(test)]
pub struct MockGcpClient {
    pub ddl_log: std::sync::Mutex<Vec<String>>,
    pub schema: Vec<BqField>,
    pub table_log: std::sync::Mutex<Vec<(String, String)>>,
    pub table_meta: Option<BqTableMeta>,
    pub dry_run_result: Option<DryRunResult>,
    pub workflow_log: std::sync::Mutex<Vec<(String, String, String)>>,
    pub scheduler_log: std::sync::Mutex<Vec<(String, String)>>,
    pub dataset_log: std::sync::Mutex<Vec<(String, String)>>,
    pub dataset_meta: Option<DatasetMeta>,
    pub routine_log: std::sync::Mutex<Vec<(String, String)>>,
    pub routine_meta: Option<RoutineMeta>,
    pub fail_on: std::sync::Mutex<Option<String>>,
}

#[cfg(test)]
impl MockGcpClient {
    pub fn new(schema: Vec<BqField>) -> Self {
        Self {
            ddl_log: std::sync::Mutex::new(Vec::new()),
            schema,
            table_log: std::sync::Mutex::new(Vec::new()),
            table_meta: None,
            dry_run_result: None,
            workflow_log: std::sync::Mutex::new(Vec::new()),
            scheduler_log: std::sync::Mutex::new(Vec::new()),
            dataset_log: std::sync::Mutex::new(Vec::new()),
            dataset_meta: None,
            routine_log: std::sync::Mutex::new(Vec::new()),
            routine_meta: None,
            fail_on: std::sync::Mutex::new(None),
        }
    }

    pub fn ddl_log(&self) -> Vec<String> {
        self.ddl_log.lock().unwrap().clone()
    }

    fn should_fail(&self, method: &str) -> bool {
        self.fail_on
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|m| m == method)
    }
}

#[cfg(test)]
#[derive(Debug, thiserror::Error)]
#[error("mock error: {0}")]
pub struct MockError(pub String);

#[cfg(test)]
impl GcpApiError for MockError {
    fn is_conflict(&self) -> bool {
        self.0.contains("409") || self.0.contains("conflict")
    }
    fn is_not_found(&self) -> bool {
        self.0.contains("404") || self.0.contains("not found")
    }
    fn is_rate_limited(&self) -> bool {
        self.0.contains("429") || self.0.contains("rate limit")
    }
}

#[cfg(test)]
impl BqOps for MockGcpClient {
    type Error = MockError;

    fn execute_ddl<'a>(
        &'a self,
        _project: &'a str,
        ddl: &'a str,
        _max_bytes_billed: Option<i64>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.ddl_log.lock().unwrap().push(ddl.to_owned());
        async {
            if self.should_fail("execute_ddl") {
                return Err(MockError("execute_ddl failed".to_owned()));
            }
            Ok(())
        }
    }

    fn get_table_schema<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _table_id: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<BqField>, Self::Error>> + Send + 'a {
        let fields: Vec<BqField> = self.schema.clone();
        async move {
            if self.should_fail("get_table_schema") {
                return Err(MockError("get_table_schema failed".to_owned()));
            }
            Ok(fields)
        }
    }

    fn create_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), body.to_string()));
        async {
            if self.should_fail("create_table") {
                return Err(MockError("create_table failed".to_owned()));
            }
            Ok(self
                .table_meta
                .clone()
                .unwrap_or_else(|| BqTableMeta::preview("TABLE")))
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn get_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _table_id: &'a str,
    ) -> impl std::future::Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        async {
            if self.should_fail("get_table") {
                return Err(MockError("get_table failed".to_owned()));
            }
            Ok(self
                .table_meta
                .clone()
                .unwrap_or_else(|| BqTableMeta::preview("TABLE")))
        }
    }

    fn patch_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), body.to_string()));
        async {
            if self.should_fail("patch_table") {
                return Err(MockError("patch_table failed".to_owned()));
            }
            Ok(BqTableMeta::preview("TABLE"))
        }
    }

    fn delete_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        table_id: &'a str,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), table_id.to_owned()));
        async {
            if self.should_fail("delete_table") {
                return Err(MockError("delete_table failed".to_owned()));
            }
            Ok(())
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn dry_run_query<'a>(
        &'a self,
        _project: &'a str,
        _sql: &'a str,
        _max_bytes_billed: Option<i64>,
    ) -> impl std::future::Future<Output = Result<DryRunResult, Self::Error>> + Send + 'a {
        async {
            if self.should_fail("dry_run_query") {
                return Err(MockError("dry_run_query failed".to_owned()));
            }
            Ok(self.dry_run_result.clone().unwrap_or(DryRunResult {
                valid: true,
                error_message: None,
                total_bytes_processed: 0,
                schema: Vec::new(),
            }))
        }
    }

    // --- Dataset mock ---

    fn create_dataset<'a>(
        &'a self,
        _project: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), body.to_string()));
        async {
            if self.should_fail("create_dataset") {
                return Err(MockError("create_dataset failed".to_owned()));
            }
            Ok(self
                .dataset_meta
                .clone()
                .unwrap_or_else(|| DatasetMeta::preview("test_dataset", "US", Some("LOGICAL"))))
        }
    }

    fn get_dataset<'a>(
        &'a self,
        _project: &'a str,
        _dataset_id: &'a str,
    ) -> impl std::future::Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        async {
            if self.should_fail("get_dataset") {
                return Err(MockError("get_dataset failed".to_owned()));
            }
            Ok(self
                .dataset_meta
                .clone()
                .unwrap_or_else(|| DatasetMeta::preview("test_dataset", "US", Some("LOGICAL"))))
        }
    }

    fn patch_dataset<'a>(
        &'a self,
        _project: &'a str,
        _dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), body.to_string()));
        async {
            if self.should_fail("patch_dataset") {
                return Err(MockError("patch_dataset failed".to_owned()));
            }
            Ok(DatasetMeta::preview("test_dataset", "US", Some("LOGICAL")))
        }
    }

    fn delete_dataset<'a>(
        &'a self,
        _project: &'a str,
        dataset_id: &'a str,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), dataset_id.to_owned()));
        async {
            if self.should_fail("delete_dataset") {
                return Err(MockError("delete_dataset failed".to_owned()));
            }
            Ok(())
        }
    }

    // --- Routine mock ---

    fn create_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), body.to_string()));
        async {
            if self.should_fail("create_routine") {
                return Err(MockError("create_routine failed".to_owned()));
            }
            Ok(self
                .routine_meta
                .clone()
                .unwrap_or_else(|| RoutineMeta::preview("test_routine", "SCALAR_FUNCTION", "SQL")))
        }
    }

    fn get_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _routine_id: &'a str,
    ) -> impl std::future::Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        async {
            if self.should_fail("get_routine") {
                return Err(MockError("get_routine failed".to_owned()));
            }
            Ok(self
                .routine_meta
                .clone()
                .unwrap_or_else(|| RoutineMeta::preview("test_routine", "SCALAR_FUNCTION", "SQL")))
        }
    }

    fn update_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("update".to_owned(), body.to_string()));
        async {
            if self.should_fail("update_routine") {
                return Err(MockError("update_routine failed".to_owned()));
            }
            Ok(RoutineMeta::preview(
                "test_routine",
                "SCALAR_FUNCTION",
                "SQL",
            ))
        }
    }

    fn delete_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        routine_id: &'a str,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), routine_id.to_owned()));
        async {
            if self.should_fail("delete_routine") {
                return Err(MockError("delete_routine failed".to_owned()));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
impl SchedulerOps for MockGcpClient {
    type Error = MockError;

    fn create_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        definition: &'a str,
        _sa: &'a str,
    ) -> impl std::future::Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "create".to_owned(),
            name.to_owned(),
            definition.to_owned(),
        ));
        async move {
            if self.should_fail("create_workflow") {
                return Err(MockError("create_workflow failed".to_owned()));
            }
            Ok(WorkflowMeta {
                name: name.to_owned(),
                state: "ACTIVE".to_owned(),
                revision_id: "1".to_owned(),
                create_time: String::new(),
                update_time: String::new(),
                service_account: String::new(),
            })
        }
    }

    fn get_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        async move {
            if self.should_fail("get_workflow") {
                return Err(MockError("get_workflow failed".to_owned()));
            }
            Ok(WorkflowMeta {
                name: name.to_owned(),
                state: "ACTIVE".to_owned(),
                revision_id: "1".to_owned(),
                create_time: String::new(),
                update_time: String::new(),
                service_account: String::new(),
            })
        }
    }

    fn update_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> impl std::future::Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "update".to_owned(),
            name.to_owned(),
            definition.to_owned(),
        ));
        async move {
            if self.should_fail("update_workflow") {
                return Err(MockError("update_workflow failed".to_owned()));
            }
            Ok(WorkflowMeta {
                name: name.to_owned(),
                state: "ACTIVE".to_owned(),
                revision_id: "2".to_owned(),
                create_time: String::new(),
                update_time: String::new(),
                service_account: String::new(),
            })
        }
    }

    fn delete_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "delete".to_owned(),
            name.to_owned(),
            String::new(),
        ));
        async {
            if self.should_fail("delete_workflow") {
                return Err(MockError("delete_workflow failed".to_owned()));
            }
            Ok(())
        }
    }

    fn create_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), body.to_string()));
        async {
            if self.should_fail("create_scheduler_job") {
                return Err(MockError("create_scheduler_job failed".to_owned()));
            }
            Ok(SchedulerJobMeta {
                name: "test-job".to_owned(),
                state: "ENABLED".to_owned(),
                schedule: String::new(),
                time_zone: String::new(),
                schedule_time: String::new(),
                last_attempt_time: String::new(),
                user_update_time: String::new(),
            })
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn get_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        _name: &'a str,
    ) -> impl std::future::Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        async {
            if self.should_fail("get_scheduler_job") {
                return Err(MockError("get_scheduler_job failed".to_owned()));
            }
            Ok(SchedulerJobMeta {
                name: "test-job".to_owned(),
                state: "ENABLED".to_owned(),
                schedule: String::new(),
                time_zone: String::new(),
                schedule_time: String::new(),
                last_attempt_time: String::new(),
                user_update_time: String::new(),
            })
        }
    }

    fn patch_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        body: &'a serde_json::Value,
    ) -> impl std::future::Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), body.to_string()));
        async move {
            if self.should_fail("patch_scheduler_job") {
                return Err(MockError("patch_scheduler_job failed".to_owned()));
            }
            Ok(SchedulerJobMeta {
                name: name.to_owned(),
                state: "ENABLED".to_owned(),
                schedule: String::new(),
                time_zone: String::new(),
                schedule_time: String::new(),
                last_attempt_time: String::new(),
                user_update_time: String::new(),
            })
        }
    }

    fn pause_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("pause".to_owned(), name.to_owned()));
        async move {
            if self.should_fail("pause_scheduler_job") {
                return Err(MockError("pause_scheduler_job failed".to_owned()));
            }
            Ok(SchedulerJobMeta {
                name: name.to_owned(),
                state: "PAUSED".to_owned(),
                schedule: String::new(),
                time_zone: String::new(),
                schedule_time: String::new(),
                last_attempt_time: String::new(),
                user_update_time: String::new(),
            })
        }
    }

    fn resume_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("resume".to_owned(), name.to_owned()));
        async move {
            if self.should_fail("resume_scheduler_job") {
                return Err(MockError("resume_scheduler_job failed".to_owned()));
            }
            Ok(SchedulerJobMeta {
                name: name.to_owned(),
                state: "ENABLED".to_owned(),
                schedule: String::new(),
                time_zone: String::new(),
                schedule_time: String::new(),
                last_attempt_time: String::new(),
                user_update_time: String::new(),
            })
        }
    }

    fn delete_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), name.to_owned()));
        async {
            if self.should_fail("delete_scheduler_job") {
                return Err(MockError("delete_scheduler_job failed".to_owned()));
            }
            Ok(())
        }
    }
}
