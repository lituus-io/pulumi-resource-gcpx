use std::collections::BTreeMap;

use crate::dataset::types::DatasetInputs;
use crate::output::OutputBuilder;
use crate::prost_util::{get_number, get_str, get_struct_fields, value_as_str};

pub fn parse_dataset_inputs(s: &prost_types::Struct) -> Result<DatasetInputs<'_>, &'static str> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID (e.g., 'my-project')")?;
    let dataset_id = get_str(&s.fields, "datasetId").ok_or("missing required field 'datasetId': provide a BigQuery dataset ID (e.g., 'analytics_prod')")?;
    let location = get_str(&s.fields, "location").ok_or("missing required field 'location': provide a BigQuery location (e.g., 'US', 'EU', 'us-central1')")?;
    let description = get_str(&s.fields, "description");
    let friendly_name = get_str(&s.fields, "friendlyName");
    let default_table_expiration_ms =
        get_number(&s.fields, "defaultTableExpirationMs").map(|n| n as i64);
    let default_partition_expiration_ms =
        get_number(&s.fields, "defaultPartitionExpirationMs").map(|n| n as i64);
    let storage_billing_model = get_str(&s.fields, "storageBillingModel");
    let max_time_travel_hours = get_number(&s.fields, "maxTimeTravelHours").map(|n| n as i64);

    let mut labels = BTreeMap::new();
    if let Some(lf) = get_struct_fields(&s.fields, "labels") {
        for (k, v) in lf {
            if let Some(val) = value_as_str(v) {
                labels.insert(k.as_str(), val);
            }
        }
    }

    Ok(DatasetInputs {
        project,
        dataset_id,
        location,
        description,
        friendly_name,
        labels,
        default_table_expiration_ms,
        default_partition_expiration_ms,
        storage_billing_model,
        max_time_travel_hours,
    })
}

pub fn build_dataset_output(
    inputs: &DatasetInputs<'_>,
    meta: &crate::bq::DatasetMeta,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("datasetId", inputs.dataset_id)
        .str("location", inputs.location)
        .num("creationTime", meta.creation_time as f64)
        .num("lastModifiedTime", meta.last_modified_time as f64)
        .str("etag", &meta.etag)
        .str(
            "storageBillingModel",
            inputs
                .storage_billing_model
                .unwrap_or(&meta.storage_billing_model),
        )
        .str_opt("description", inputs.description)
        .str_opt("friendlyName", inputs.friendly_name)
        .labels("labels", &inputs.labels)
        .num_opt(
            "defaultTableExpirationMs",
            inputs.default_table_expiration_ms,
        )
        .num_opt(
            "defaultPartitionExpirationMs",
            inputs.default_partition_expiration_ms,
        )
        .num_opt("maxTimeTravelHours", inputs.max_time_travel_hours)
        .build()
}
