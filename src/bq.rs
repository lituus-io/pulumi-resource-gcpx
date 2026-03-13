use std::collections::BTreeMap;
use std::future::Future;

use serde::Deserialize;

/// Owned field from BQ API response (distinct from zero-copy SchemaField<'a>).
#[derive(Clone)]
pub struct BqField {
    pub name: String,
    pub field_type: String,
    pub mode: String,
    pub description: String,
    pub fields: Vec<BqField>,
}

/// Metadata returned from BQ table API.
#[derive(Clone)]
pub struct BqTableMeta {
    pub table_type: String,
    pub description: String,
    pub friendly_name: String,
    pub labels: BTreeMap<String, String>,
    pub location: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
    pub num_rows: i64,
    pub num_bytes: i64,
    pub etag: String,
    pub self_link: String,
    pub expiration_time: Option<i64>,
    pub schema_fields: Vec<BqField>,
}

/// Result of a BigQuery dry-run query.
#[derive(Clone)]
pub struct DryRunResult {
    pub valid: bool,
    pub error_message: Option<String>,
    pub total_bytes_processed: i64,
    pub schema: Vec<BqField>,
}

/// Metadata returned from BQ dataset API.
#[derive(Clone)]
pub struct DatasetMeta {
    pub dataset_id: String,
    pub location: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
    pub etag: String,
    pub description: String,
    pub friendly_name: String,
    pub labels: BTreeMap<String, String>,
    pub default_table_expiration_ms: Option<i64>,
    pub default_partition_expiration_ms: Option<i64>,
    pub storage_billing_model: String,
    pub max_time_travel_hours: Option<i64>,
}

/// Metadata returned from BQ routine API.
#[derive(Clone)]
pub struct RoutineMeta {
    pub routine_id: String,
    pub routine_type: String,
    pub language: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
    pub etag: String,
}

impl DatasetMeta {
    /// Constructs a preview placeholder for dry-run mode.
    pub fn preview(dataset_id: &str, location: &str, billing: Option<&str>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            location: location.to_owned(),
            creation_time: 0,
            last_modified_time: 0,
            etag: String::new(),
            description: String::new(),
            friendly_name: String::new(),
            labels: BTreeMap::new(),
            default_table_expiration_ms: None,
            default_partition_expiration_ms: None,
            storage_billing_model: billing.unwrap_or("LOGICAL").to_owned(),
            max_time_travel_hours: None,
        }
    }
}

impl BqTableMeta {
    /// Constructs a preview placeholder for dry-run mode.
    pub fn preview(table_type: &str) -> Self {
        Self {
            table_type: table_type.to_owned(),
            description: String::new(),
            friendly_name: String::new(),
            labels: BTreeMap::new(),
            location: "US".to_owned(),
            creation_time: 0,
            last_modified_time: 0,
            num_rows: 0,
            num_bytes: 0,
            etag: String::new(),
            self_link: String::new(),
            expiration_time: None,
            schema_fields: Vec::new(),
        }
    }
}

impl RoutineMeta {
    /// Constructs a preview placeholder for dry-run mode.
    pub fn preview(routine_id: &str, routine_type: &str, language: &str) -> Self {
        Self {
            routine_id: routine_id.to_owned(),
            routine_type: routine_type.to_owned(),
            language: language.to_owned(),
            creation_time: 0,
            last_modified_time: 0,
            etag: String::new(),
        }
    }
}

/// Trait for BigQuery operations. Generic over implementation for testing.
///
/// Uses RPITIT (return position impl trait in trait) — no boxing.
/// All methods use explicit `'a` lifetime for true zero-copy.
pub trait BqOps: Send + Sync + 'static {
    type Error: crate::gcp_client::GcpApiError;

    fn execute_ddl<'a>(
        &'a self,
        project: &'a str,
        ddl: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn get_table_schema<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<Vec<BqField>, Self::Error>> + Send + 'a;

    fn create_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn get_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn patch_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn delete_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn dry_run_query<'a>(
        &'a self,
        project: &'a str,
        sql: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<DryRunResult, Self::Error>> + Send + 'a;

    // --- Dataset operations ---

    fn create_dataset<'a>(
        &'a self,
        project: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    fn get_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    fn patch_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    fn delete_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    // --- Routine operations ---

    fn create_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn get_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn update_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn delete_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

// --- Deserialization types for BQ API responses ---

#[derive(Deserialize)]
pub struct BqTableResponse {
    #[serde(default)]
    pub schema: Option<BqSchemaResponse>,
    #[serde(default, rename = "type")]
    pub table_type: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, rename = "friendlyName")]
    pub friendly_name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub location: String,
    #[serde(default, rename = "creationTime")]
    pub creation_time: String,
    #[serde(default, rename = "lastModifiedTime")]
    pub last_modified_time: String,
    #[serde(default, rename = "numRows")]
    pub num_rows: String,
    #[serde(default, rename = "numBytes")]
    pub num_bytes: String,
    #[serde(default)]
    pub etag: String,
    #[serde(default, rename = "selfLink")]
    pub self_link: String,
    #[serde(default, rename = "expirationTime")]
    pub expiration_time: Option<String>,
}

#[derive(Deserialize)]
pub struct BqSchemaResponse {
    #[serde(default)]
    pub fields: Vec<BqFieldResponse>,
}

#[derive(Deserialize)]
pub struct BqFieldResponse {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub fields: Vec<BqFieldResponse>,
}

#[derive(Deserialize)]
pub struct BqDryRunResponse {
    #[serde(default)]
    pub statistics: Option<BqDryRunStats>,
    #[serde(default)]
    pub schema: Option<BqSchemaResponse>,
}

#[derive(Deserialize)]
pub struct BqDryRunStats {
    #[serde(default, rename = "totalBytesProcessed")]
    pub total_bytes_processed: String,
}

#[derive(Deserialize)]
pub struct BqDatasetResponse {
    #[serde(default, rename = "datasetReference")]
    pub dataset_reference: BqDatasetRef,
    #[serde(default)]
    pub location: String,
    #[serde(default, rename = "creationTime")]
    pub creation_time: String,
    #[serde(default, rename = "lastModifiedTime")]
    pub last_modified_time: String,
    #[serde(default)]
    pub etag: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, rename = "friendlyName")]
    pub friendly_name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default, rename = "defaultTableExpirationMs")]
    pub default_table_expiration_ms: Option<String>,
    #[serde(default, rename = "defaultPartitionExpirationMs")]
    pub default_partition_expiration_ms: Option<String>,
    #[serde(default, rename = "storageBillingModel")]
    pub storage_billing_model: String,
    #[serde(default, rename = "maxTimeTravelHours")]
    pub max_time_travel_hours: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct BqDatasetRef {
    #[serde(default, rename = "datasetId")]
    pub dataset_id: String,
}

#[derive(Deserialize)]
pub struct BqRoutineResponse {
    #[serde(default, rename = "routineReference")]
    pub routine_reference: BqRoutineRef,
    #[serde(default, rename = "routineType")]
    pub routine_type: String,
    #[serde(default)]
    pub language: String,
    #[serde(default, rename = "creationTime")]
    pub creation_time: String,
    #[serde(default, rename = "lastModifiedTime")]
    pub last_modified_time: String,
    #[serde(default)]
    pub etag: String,
}

#[derive(Deserialize, Default)]
pub struct BqRoutineRef {
    #[serde(default, rename = "routineId")]
    pub routine_id: String,
}

pub fn convert_bq_fields(fields: Vec<BqFieldResponse>) -> Vec<BqField> {
    fields
        .into_iter()
        .map(|f| BqField {
            name: f.name,
            field_type: f.field_type,
            mode: f.mode,
            description: f.description,
            fields: convert_bq_fields(f.fields),
        })
        .collect()
}

pub fn convert_table_response(resp: BqTableResponse) -> BqTableMeta {
    BqTableMeta {
        table_type: resp.table_type,
        description: resp.description,
        friendly_name: resp.friendly_name,
        labels: resp.labels,
        location: resp.location,
        creation_time: resp.creation_time.parse().unwrap_or(0),
        last_modified_time: resp.last_modified_time.parse().unwrap_or(0),
        num_rows: resp.num_rows.parse().unwrap_or(0),
        num_bytes: resp.num_bytes.parse().unwrap_or(0),
        etag: resp.etag,
        self_link: resp.self_link,
        expiration_time: resp.expiration_time.and_then(|s| s.parse().ok()),
        schema_fields: resp
            .schema
            .map(|s| convert_bq_fields(s.fields))
            .unwrap_or_default(),
    }
}

pub fn convert_dataset_response(resp: BqDatasetResponse) -> DatasetMeta {
    DatasetMeta {
        dataset_id: resp.dataset_reference.dataset_id,
        location: resp.location,
        creation_time: resp.creation_time.parse().unwrap_or(0),
        last_modified_time: resp.last_modified_time.parse().unwrap_or(0),
        etag: resp.etag,
        description: resp.description,
        friendly_name: resp.friendly_name,
        labels: resp.labels,
        default_table_expiration_ms: resp
            .default_table_expiration_ms
            .and_then(|s| s.parse().ok()),
        default_partition_expiration_ms: resp
            .default_partition_expiration_ms
            .and_then(|s| s.parse().ok()),
        storage_billing_model: resp.storage_billing_model,
        max_time_travel_hours: resp.max_time_travel_hours.and_then(|s| s.parse().ok()),
    }
}

pub fn convert_routine_response(resp: BqRoutineResponse) -> RoutineMeta {
    RoutineMeta {
        routine_id: resp.routine_reference.routine_id,
        routine_type: resp.routine_type,
        language: resp.language,
        creation_time: resp.creation_time.parse().unwrap_or(0),
        last_modified_time: resp.last_modified_time.parse().unwrap_or(0),
        etag: resp.etag,
    }
}
