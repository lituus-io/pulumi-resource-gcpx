use std::collections::BTreeMap;

pub struct TableInputs<'a> {
    pub project: &'a str,
    pub dataset: &'a str,
    pub table_id: &'a str,
    pub description: Option<&'a str>,
    pub friendly_name: Option<&'a str>,
    pub labels: BTreeMap<&'a str, &'a str>,
    pub view: Option<ViewInputs<'a>>,
    pub materialized_view: Option<MaterializedViewInputs<'a>>,
    pub time_partitioning: Option<TimePartitioningInputs<'a>>,
    pub range_partitioning: Option<RangePartitioningInputs<'a>>,
    pub clusterings: Vec<&'a str>,
    pub deletion_protection: Option<bool>,
    pub expiration_time: Option<i64>,
    pub encryption_kms_key: Option<&'a str>,
    pub storage_billing_model: Option<&'a str>,
    pub max_staleness: Option<&'a str>,
    pub external_data_config: Option<ExternalDataConfig<'a>>,
}

pub struct ExternalDataConfig<'a> {
    pub source_uris: Vec<&'a str>,
    pub source_format: &'a str,
    pub autodetect: Option<bool>,
    pub connection_id: Option<&'a str>,
    pub csv_skip_leading_rows: Option<i64>,
}

pub struct ViewInputs<'a> {
    pub query: &'a str,
    pub use_legacy_sql: Option<bool>,
}

pub struct MaterializedViewInputs<'a> {
    pub query: &'a str,
    pub enable_refresh: Option<bool>,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness: Option<&'a str>,
}

pub struct TimePartitioningInputs<'a> {
    pub partition_type: &'a str,
    pub field: Option<&'a str>,
    pub expiration_ms: Option<i64>,
}

pub struct RangePartitioningInputs<'a> {
    pub field: &'a str,
    pub start: i64,
    pub end: i64,
    pub interval: i64,
}
