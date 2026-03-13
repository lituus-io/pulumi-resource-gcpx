use crate::schema::types::CheckFailure;
use crate::table::types::TableInputs;

pub fn validate_table(inputs: &TableInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if inputs.project.is_empty() {
        failures.push(CheckFailure {
            property: "project".into(),
            reason:
                "project cannot be empty: provide a valid GCP project ID (e.g., 'my-gcp-project')"
                    .into(),
        });
    }
    if inputs.dataset.is_empty() {
        failures.push(CheckFailure {
            property: "dataset".into(),
            reason:
                "dataset cannot be empty: provide a valid BigQuery dataset ID (e.g., 'my_dataset')"
                    .into(),
        });
    }
    if inputs.table_id.is_empty() {
        failures.push(CheckFailure {
            property: "tableId".into(),
            reason: "tableId cannot be empty: provide a valid BigQuery table name (e.g., 'orders')"
                .into(),
        });
    }

    // View and materialized view are mutually exclusive.
    if inputs.view.is_some() && inputs.materialized_view.is_some() {
        failures.push(CheckFailure {
            property: "view".into(),
            reason: "view and materializedView are mutually exclusive".into(),
        });
    }

    // View query must not be empty.
    if let Some(ref v) = inputs.view {
        if v.query.is_empty() {
            failures.push(CheckFailure {
                property: "view.query".into(),
                reason: "view query must not be empty".into(),
            });
        }
    }

    // MV query must not be empty.
    if let Some(ref mv) = inputs.materialized_view {
        if mv.query.is_empty() {
            failures.push(CheckFailure {
                property: "materializedView.query".into(),
                reason: "materialized view query must not be empty".into(),
            });
        }
    }

    // Time and range partitioning are mutually exclusive.
    if inputs.time_partitioning.is_some() && inputs.range_partitioning.is_some() {
        failures.push(CheckFailure {
            property: "timePartitioning".into(),
            reason: "timePartitioning and rangePartitioning are mutually exclusive".into(),
        });
    }

    // Clustering: max 4 fields.
    if inputs.clusterings.len() > 4 {
        failures.push(CheckFailure {
            property: "clusterings".into(),
            reason: "clusterings exceeds BigQuery limit: maximum 4 fields allowed".into(),
        });
    }

    // Validate storageBillingModel if present.
    if let Some(sbm) = inputs.storage_billing_model {
        if !matches!(sbm, "LOGICAL" | "PHYSICAL") {
            failures.push(CheckFailure {
                property: "storageBillingModel".into(),
                reason: "storageBillingModel must be 'LOGICAL' or 'PHYSICAL' (case-sensitive)"
                    .into(),
            });
        }
    }

    // maxStaleness only valid on materialized views.
    if inputs.max_staleness.is_some() && inputs.materialized_view.is_none() {
        failures.push(CheckFailure {
            property: "maxStaleness".into(),
            reason: "maxStaleness is only valid for materialized views".into(),
        });
    }

    // Views/MVs cannot have partitioning or clustering.
    if inputs.view.is_some() {
        if inputs.time_partitioning.is_some() || inputs.range_partitioning.is_some() {
            failures.push(CheckFailure {
                property: "view".into(),
                reason: "views cannot have partitioning".into(),
            });
        }
        if !inputs.clusterings.is_empty() {
            failures.push(CheckFailure {
                property: "view".into(),
                reason: "views cannot have clustering".into(),
            });
        }
    }

    // External tables cannot be views or materialized views.
    if inputs.external_data_config.is_some() {
        if inputs.view.is_some() {
            failures.push(CheckFailure {
                property: "externalDataConfiguration".into(),
                reason: "externalDataConfiguration is mutually exclusive with view: external tables cannot be views".into(),
            });
        }
        if inputs.materialized_view.is_some() {
            failures.push(CheckFailure {
                property: "externalDataConfiguration".into(),
                reason: "externalDataConfiguration is mutually exclusive with materializedView: external tables cannot be materialized views".into(),
            });
        }
    }

    // External tables must have at least one sourceUri.
    if let Some(ref edc) = inputs.external_data_config {
        if edc.source_uris.is_empty() {
            failures.push(CheckFailure {
                property: "externalDataConfiguration.sourceUris".into(),
                reason: "externalDataConfiguration.sourceUris must contain at least one URI (e.g., 'gs://bucket/path/*.csv')".into(),
            });
        }
    }

    // Cross-field constraints.

    // MV with enableRefresh cannot be an external table.
    if let Some(ref mv) = inputs.materialized_view {
        if mv.enable_refresh == Some(true) && inputs.external_data_config.is_some() {
            failures.push(CheckFailure {
                property: "materializedView.enableRefresh".into(),
                reason: "enableRefresh is not supported on external tables".into(),
            });
        }
    }

    // deletionProtection + expirationTime conflict.
    if inputs.deletion_protection == Some(true) && inputs.expiration_time.is_some() {
        failures.push(CheckFailure {
            property: "deletionProtection".into(),
            reason: "deletionProtection cannot be used with expirationTime: expiration would bypass deletion protection".into(),
        });
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::types::{MaterializedViewInputs, ViewInputs};
    use std::collections::BTreeMap;

    fn base_inputs<'a>() -> TableInputs<'a> {
        TableInputs {
            project: "proj",
            dataset: "ds",
            table_id: "tbl",
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
    fn valid_table() {
        let inputs = base_inputs();
        let failures = validate_table(&inputs);
        assert!(failures.is_empty());
    }

    #[test]
    fn empty_project() {
        let mut inputs = base_inputs();
        inputs.project = "";
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("project cannot be empty")));
    }

    #[test]
    fn view_and_mv_mutually_exclusive() {
        let mut inputs = base_inputs();
        inputs.view = Some(ViewInputs {
            query: "SELECT 1",
            use_legacy_sql: None,
        });
        inputs.materialized_view = Some(MaterializedViewInputs {
            query: "SELECT 1",
            enable_refresh: None,
            refresh_interval_ms: None,
            max_staleness: None,
        });
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("mutually exclusive")));
    }

    #[test]
    fn view_empty_query() {
        let mut inputs = base_inputs();
        inputs.view = Some(ViewInputs {
            query: "",
            use_legacy_sql: None,
        });
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("view query must not be empty")));
    }

    #[test]
    fn too_many_clustering_fields() {
        let mut inputs = base_inputs();
        inputs.clusterings = vec!["a", "b", "c", "d", "e"];
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("maximum 4 fields allowed")));
    }

    #[test]
    fn external_table_with_view_rejected() {
        use crate::table::types::ExternalDataConfig;
        let mut inputs = base_inputs();
        inputs.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/file.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: None,
            csv_skip_leading_rows: None,
        });
        inputs.view = Some(ViewInputs {
            query: "SELECT 1",
            use_legacy_sql: None,
        });
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("mutually exclusive with view")));
    }

    #[test]
    fn external_table_empty_source_uris() {
        use crate::table::types::ExternalDataConfig;
        let mut inputs = base_inputs();
        inputs.external_data_config = Some(ExternalDataConfig {
            source_uris: vec![],
            source_format: "CSV",
            autodetect: None,
            connection_id: None,
            csv_skip_leading_rows: None,
        });
        let failures = validate_table(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("must contain at least one URI")));
    }

    #[test]
    fn external_table_valid() {
        use crate::table::types::ExternalDataConfig;
        let mut inputs = base_inputs();
        inputs.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/file.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: Some("projects/p/locations/us/connections/conn"),
            csv_skip_leading_rows: Some(1),
        });
        let failures = validate_table(&inputs);
        assert!(failures.is_empty());
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn validate_table_never_panics(
            project in "[ -~]{0,30}",
            dataset in "[ -~]{0,30}",
            table_id in "[ -~]{0,30}",
        ) {
            let inputs = TableInputs {
                project: &project,
                dataset: &dataset,
                table_id: &table_id,
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
            };
            let _ = std::hint::black_box(validate_table(&inputs));
        }

        #[test]
        fn validate_table_with_storage_billing(sbm in "[ -~]{0,20}") {
            let inputs = TableInputs {
                project: "proj",
                dataset: "ds",
                table_id: "tbl",
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
                storage_billing_model: Some(&sbm),
                max_staleness: None,
                external_data_config: None,
            };
            let failures = validate_table(&inputs);
            // Only LOGICAL and PHYSICAL are valid
            if sbm == "LOGICAL" || sbm == "PHYSICAL" {
                prop_assert!(failures.iter().all(|f| !f.property.contains("storageBillingModel")));
            }
        }
    }
}
