use crate::dataset::types::DatasetInputs;
use crate::json_body::JsonBody;

pub fn build_create_body(inputs: &DatasetInputs<'_>) -> serde_json::Value {
    JsonBody::new()
        .object(
            "datasetReference",
            serde_json::json!({
                "projectId": inputs.project,
                "datasetId": inputs.dataset_id,
            }),
        )
        .str("location", inputs.location)
        .str_opt("description", inputs.description)
        .str_opt("friendlyName", inputs.friendly_name)
        .labels("labels", &inputs.labels)
        .num_as_str_opt(
            "defaultTableExpirationMs",
            inputs.default_table_expiration_ms,
        )
        .num_as_str_opt(
            "defaultPartitionExpirationMs",
            inputs.default_partition_expiration_ms,
        )
        .str_opt("storageBillingModel", inputs.storage_billing_model)
        .num_as_str_opt("maxTimeTravelHours", inputs.max_time_travel_hours)
        .build()
}

pub fn build_patch_body(inputs: &DatasetInputs<'_>, update_keys: &[&str]) -> serde_json::Value {
    let mut body = JsonBody::new();

    for key in update_keys {
        match *key {
            "description" => {
                body = body.str("description", inputs.description.unwrap_or(""));
            }
            "friendlyName" => {
                body = body.str("friendlyName", inputs.friendly_name.unwrap_or(""));
            }
            "labels" => {
                let labels_obj: serde_json::Map<String, serde_json::Value> = inputs
                    .labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                    .collect();
                body = body.object("labels", serde_json::Value::Object(labels_obj));
            }
            "defaultTableExpirationMs" => {
                body = body.num_as_str_opt(
                    "defaultTableExpirationMs",
                    inputs.default_table_expiration_ms,
                );
            }
            "defaultPartitionExpirationMs" => {
                body = body.num_as_str_opt(
                    "defaultPartitionExpirationMs",
                    inputs.default_partition_expiration_ms,
                );
            }
            "storageBillingModel" => {
                body = body.str_opt("storageBillingModel", inputs.storage_billing_model);
            }
            "maxTimeTravelHours" => {
                body = body.num_as_str_opt("maxTimeTravelHours", inputs.max_time_travel_hours);
            }
            _ => {}
        }
    }

    body.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base<'a>() -> DatasetInputs<'a> {
        DatasetInputs {
            project: "p",
            dataset_id: "d",
            location: "US",
            description: None,
            friendly_name: None,
            labels: BTreeMap::new(),
            default_table_expiration_ms: None,
            default_partition_expiration_ms: None,
            storage_billing_model: None,
            max_time_travel_hours: None,
        }
    }

    #[test]
    fn create_body_basic() {
        let body = build_create_body(&base());
        assert_eq!(body["datasetReference"]["datasetId"], "d");
        assert_eq!(body["location"], "US");
    }

    #[test]
    fn create_body_with_labels() {
        let mut inputs = base();
        inputs.labels.insert("env", "prod");
        let body = build_create_body(&inputs);
        assert_eq!(body["labels"]["env"], "prod");
    }

    #[test]
    fn patch_body_description() {
        let mut inputs = base();
        inputs.description = Some("updated");
        let body = build_patch_body(&inputs, &["description"]);
        assert_eq!(body["description"], "updated");
    }
}
