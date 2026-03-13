use crate::json_body::JsonBody;
use crate::table::types::TableInputs;

/// Build JSON body for BQ tables.insert API call.
pub fn build_create_body(inputs: &TableInputs<'_>) -> serde_json::Value {
    let mut body = JsonBody::new()
        .object(
            "tableReference",
            serde_json::json!({
                "projectId": inputs.project,
                "datasetId": inputs.dataset,
                "tableId": inputs.table_id,
            }),
        )
        .str_opt("description", inputs.description)
        .str_opt("friendlyName", inputs.friendly_name)
        .labels("labels", &inputs.labels)
        .num_as_str_opt("expirationTime", inputs.expiration_time);

    if let Some(ref v) = inputs.view {
        let view_obj = JsonBody::new()
            .str("query", v.query)
            .bool_opt("useLegacySql", v.use_legacy_sql)
            .build();
        body = body.object("view", view_obj);
    }

    if let Some(ref mv) = inputs.materialized_view {
        let mv_obj = JsonBody::new()
            .str("query", mv.query)
            .bool_opt("enableRefresh", mv.enable_refresh)
            .num_as_str_opt("refreshIntervalMs", mv.refresh_interval_ms)
            .str_opt("maxStaleness", mv.max_staleness)
            .build();
        body = body.object("materializedView", mv_obj);
    }

    if let Some(ref tp) = inputs.time_partitioning {
        let tp_obj = JsonBody::new()
            .str("type", tp.partition_type)
            .str_opt("field", tp.field)
            .num_as_str_opt("expirationMs", tp.expiration_ms)
            .build();
        body = body.object("timePartitioning", tp_obj);
    }

    if let Some(ref rp) = inputs.range_partitioning {
        body = body.object(
            "rangePartitioning",
            serde_json::json!({
                "field": rp.field,
                "range": {
                    "start": rp.start.to_string(),
                    "end": rp.end.to_string(),
                    "interval": rp.interval.to_string(),
                },
            }),
        );
    }

    if !inputs.clusterings.is_empty() {
        body = body.object(
            "clustering",
            serde_json::json!({ "fields": inputs.clusterings }),
        );
    }

    if let Some(kms) = inputs.encryption_kms_key {
        body = body.object(
            "encryptionConfiguration",
            serde_json::json!({ "kmsKeyName": kms }),
        );
    }

    body = body.str_opt("storageBillingModel", inputs.storage_billing_model);

    if let Some(ref edc) = inputs.external_data_config {
        let mut edc_body = JsonBody::new()
            .object("sourceUris", serde_json::json!(edc.source_uris))
            .str("sourceFormat", edc.source_format)
            .bool_opt("autodetect", edc.autodetect)
            .str_opt("connectionId", edc.connection_id);
        if let Some(skip) = edc.csv_skip_leading_rows {
            edc_body = edc_body.object(
                "csvOptions",
                serde_json::json!({ "skipLeadingRows": skip.to_string() }),
            );
        }
        body = body.object("externalDataConfiguration", edc_body.build());
    }

    body.build()
}

/// Build JSON body for BQ tables.patch API call (only updatable fields).
pub fn build_patch_body(inputs: &TableInputs<'_>, update_keys: &[&str]) -> serde_json::Value {
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
            "expirationTime" => {
                body = body.num_as_str_opt("expirationTime", inputs.expiration_time);
            }
            "deletionProtection" => {
                body = body.bool_opt("deletionProtection", inputs.deletion_protection);
            }
            "clusterings" => {
                body = body.object(
                    "clustering",
                    serde_json::json!({ "fields": inputs.clusterings }),
                );
            }
            "storageBillingModel" => {
                body = body.str_opt("storageBillingModel", inputs.storage_billing_model);
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

    fn base<'a>() -> TableInputs<'a> {
        TableInputs {
            project: "p",
            dataset: "d",
            table_id: "t",
            description: None,
            friendly_name: None,
            labels: BTreeMap::new(),
            view: None,
            materialized_view: None,
            time_partitioning: None,
            range_partitioning: None,
            clusterings: vec![],
            deletion_protection: None,
            expiration_time: None,
            encryption_kms_key: None,
            storage_billing_model: None,
            max_staleness: None,
            external_data_config: None,
        }
    }

    #[test]
    fn create_body_basic() {
        let inputs = base();
        let body = build_create_body(&inputs);
        assert_eq!(body["tableReference"]["tableId"], "t");
    }

    #[test]
    fn create_body_with_view() {
        use crate::table::types::ViewInputs;
        let mut inputs = base();
        inputs.view = Some(ViewInputs {
            query: "SELECT 1",
            use_legacy_sql: Some(false),
        });
        let body = build_create_body(&inputs);
        assert_eq!(body["view"]["query"], "SELECT 1");
        assert_eq!(body["view"]["useLegacySql"], false);
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

    #[test]
    fn create_body_with_external_data_config() {
        use crate::table::types::ExternalDataConfig;
        let mut inputs = base();
        inputs.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/file.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: Some("projects/p/locations/us/connections/c"),
            csv_skip_leading_rows: Some(1),
        });
        let body = build_create_body(&inputs);
        let edc = &body["externalDataConfiguration"];
        assert_eq!(edc["sourceFormat"], "CSV");
        assert_eq!(edc["autodetect"], true);
        assert_eq!(edc["sourceUris"][0], "gs://bucket/file.csv");
        assert_eq!(edc["connectionId"], "projects/p/locations/us/connections/c");
        assert_eq!(edc["csvOptions"]["skipLeadingRows"], "1");
    }
}
