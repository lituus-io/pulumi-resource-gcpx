use std::collections::BTreeMap;

pub struct DatasetInputs<'a> {
    pub project: &'a str,
    pub dataset_id: &'a str,
    pub location: &'a str,
    pub description: Option<&'a str>,
    pub friendly_name: Option<&'a str>,
    pub labels: BTreeMap<&'a str, &'a str>,
    pub default_table_expiration_ms: Option<i64>,
    pub default_partition_expiration_ms: Option<i64>,
    pub storage_billing_model: Option<&'a str>,
    pub max_time_travel_hours: Option<i64>,
}
