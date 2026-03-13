//! Centralized error conversion traits for tonic::Status.
//!
//! Eliminates `.map_err(|e| Status::internal(e.to_string()))` boilerplate
//! across all handler files.

use std::fmt;
use tonic::Status;

/// Converts `Result<T, E>` to `Result<T, Status>` with `Status::internal`.
#[allow(clippy::result_large_err)]
pub trait IntoStatus<T> {
    fn status_internal(self) -> Result<T, Status>;
    fn status_invalid(self) -> Result<T, Status>;
}

/// Converts `Result<T, E>` to `Result<T, Status>` with a prefix message.
#[allow(clippy::result_large_err)]
pub trait IntoStatusWith<T> {
    fn status_internal_with(self, prefix: &str) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> IntoStatus<T> for Result<T, E> {
    fn status_internal(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(e.to_string()))
    }

    fn status_invalid(self) -> Result<T, Status> {
        self.map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

impl<T, E: std::fmt::Display> IntoStatusWith<T> for Result<T, E> {
    fn status_internal_with(self, prefix: &str) -> Result<T, Status> {
        self.map_err(|e| Status::internal(format!("{}: {}", prefix, e)))
    }
}

/// Structured error types with actionable suggestions.
#[derive(Debug)]
pub enum GcpxError {
    /// BigQuery or Cloud API rate limit exceeded.
    RateLimited { project: String, api: String },
    /// Schema evolution (ALTER TABLE) failed.
    SchemaEvolutionFailed {
        table: String,
        reason: String,
        suggestion: String,
    },
    /// dbt model ref resolution failed — a referenced model is missing.
    DbtResolutionFailed { model: String, missing_ref: String },
    /// GCP authentication token has expired.
    AuthExpired,
}

impl fmt::Display for GcpxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RateLimited { project, api } => {
                write!(
                    f,
                    "Rate limited on {api} for project '{project}'. \
                     Suggestion: add retry logic with exponential backoff, \
                     or reduce concurrent operations."
                )
            }
            Self::SchemaEvolutionFailed {
                table,
                reason,
                suggestion,
            } => {
                write!(
                    f,
                    "Schema evolution failed for table '{table}': {reason}. \
                     Suggestion: {suggestion}"
                )
            }
            Self::DbtResolutionFailed { model, missing_ref } => {
                write!(
                    f,
                    "dbt model '{model}' references '{missing_ref}' which is not declared. \
                     Suggestion: add '{missing_ref}' to declaredModels in the dbt:Project resource, \
                     or create a dbt:Model resource for it."
                )
            }
            Self::AuthExpired => {
                write!(
                    f,
                    "GCP authentication token has expired. \
                     Suggestion: re-authenticate with `gcloud auth application-default login` \
                     or refresh your service account credentials."
                )
            }
        }
    }
}

impl std::error::Error for GcpxError {}

impl GcpxError {
    /// Convert to a tonic Status with appropriate gRPC status code.
    pub fn into_status(self) -> Status {
        match &self {
            Self::RateLimited { .. } => Status::resource_exhausted(self.to_string()),
            Self::SchemaEvolutionFailed { .. } => Status::failed_precondition(self.to_string()),
            Self::DbtResolutionFailed { .. } => Status::invalid_argument(self.to_string()),
            Self::AuthExpired => Status::unauthenticated(self.to_string()),
        }
    }
}

/// Blanket IntoStatus for GcpxError.
impl<T> From<GcpxError> for Result<T, Status> {
    fn from(err: GcpxError) -> Self {
        Err(err.into_status())
    }
}
