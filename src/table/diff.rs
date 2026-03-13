use crate::resource::DiffResult;
use crate::table::types::TableInputs;

/// Compute diff for Table resource.
/// Schema is ALWAYS ignored. Views/MVs trigger replace on query change.
pub fn compute_table_diff(old: &TableInputs<'_>, new: &TableInputs<'_>) -> DiffResult {
    let mut replace_keys = Vec::new();
    let mut update_keys = Vec::new();

    // Replace triggers.
    if old.project != new.project {
        replace_keys.push("project");
    }
    if old.dataset != new.dataset {
        replace_keys.push("dataset");
    }
    if old.table_id != new.table_id {
        replace_keys.push("tableId");
    }

    // View <-> non-view toggle.
    if old.view.is_some() != new.view.is_some() {
        replace_keys.push("view");
    }
    // MV <-> non-MV toggle.
    if old.materialized_view.is_some() != new.materialized_view.is_some() {
        replace_keys.push("materializedView");
    }

    // View query change -> replace.
    if let (Some(ref ov), Some(ref nv)) = (&old.view, &new.view) {
        if ov.query != nv.query {
            replace_keys.push("view.query");
        }
        if ov.use_legacy_sql != nv.use_legacy_sql {
            replace_keys.push("view.useLegacySql");
        }
    }

    // MV query change -> replace.
    if let (Some(ref omv), Some(ref nmv)) = (&old.materialized_view, &new.materialized_view) {
        if omv.query != nmv.query {
            replace_keys.push("materializedView.query");
        }
    }

    // Partition type/field change -> replace.
    match (&old.time_partitioning, &new.time_partitioning) {
        (Some(otp), Some(ntp)) => {
            if otp.partition_type != ntp.partition_type || otp.field != ntp.field {
                replace_keys.push("timePartitioning");
            }
            // expiration_ms is updatable.
            if otp.expiration_ms != ntp.expiration_ms {
                update_keys.push("timePartitioning.expirationMs");
            }
        }
        (None, Some(_)) | (Some(_), None) => {
            replace_keys.push("timePartitioning");
        }
        (None, None) => {}
    }

    // Range partitioning change -> replace.
    match (&old.range_partitioning, &new.range_partitioning) {
        (Some(_), None) | (None, Some(_)) => {
            replace_keys.push("rangePartitioning");
        }
        (Some(orp), Some(nrp)) => {
            if orp.field != nrp.field
                || orp.start != nrp.start
                || orp.end != nrp.end
                || orp.interval != nrp.interval
            {
                replace_keys.push("rangePartitioning");
            }
        }
        (None, None) => {}
    }

    // Encryption change -> replace.
    if old.encryption_kms_key != new.encryption_kms_key {
        replace_keys.push("encryptionKmsKey");
    }

    // External data config change -> replace.
    match (&old.external_data_config, &new.external_data_config) {
        (Some(_), None) | (None, Some(_)) => {
            replace_keys.push("externalDataConfiguration");
        }
        (Some(oedc), Some(nedc)) => {
            if oedc.source_uris != nedc.source_uris
                || oedc.source_format != nedc.source_format
                || oedc.autodetect != nedc.autodetect
                || oedc.connection_id != nedc.connection_id
                || oedc.csv_skip_leading_rows != nedc.csv_skip_leading_rows
            {
                replace_keys.push("externalDataConfiguration");
            }
        }
        (None, None) => {}
    }

    // Update triggers.
    if old.description != new.description {
        update_keys.push("description");
    }
    if old.friendly_name != new.friendly_name {
        update_keys.push("friendlyName");
    }
    if old.labels != new.labels {
        update_keys.push("labels");
    }
    if old.expiration_time != new.expiration_time {
        update_keys.push("expirationTime");
    }
    if old.deletion_protection != new.deletion_protection {
        update_keys.push("deletionProtection");
    }
    if old.clusterings != new.clusterings {
        update_keys.push("clusterings");
    }

    // Storage billing model change.
    if old.storage_billing_model != new.storage_billing_model {
        update_keys.push("storageBillingModel");
    }
    // maxStaleness change.
    if old.max_staleness != new.max_staleness {
        update_keys.push("maxStaleness");
    }

    // MV refresh settings are updatable.
    if let (Some(ref omv), Some(ref nmv)) = (&old.materialized_view, &new.materialized_view) {
        if omv.enable_refresh != nmv.enable_refresh {
            update_keys.push("materializedView.enableRefresh");
        }
        if omv.refresh_interval_ms != nmv.refresh_interval_ms {
            update_keys.push("materializedView.refreshIntervalMs");
        }
        if omv.max_staleness != nmv.max_staleness {
            update_keys.push("materializedView.maxStaleness");
        }
    }

    DiffResult {
        replace_keys,
        update_keys,
    }
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
    fn no_changes() {
        let old = base();
        let new = base();
        let diff = compute_table_diff(&old, &new);
        assert!(!diff.has_changes());
    }

    #[test]
    fn description_update() {
        let old = base();
        let mut new = base();
        new.description = Some("new desc");
        let diff = compute_table_diff(&old, &new);
        assert!(diff.has_changes());
        assert!(!diff.needs_replace());
        assert!(diff.update_keys.contains(&"description"));
    }

    #[test]
    fn project_replace() {
        let old = base();
        let mut new = base();
        new.project = "other";
        let diff = compute_table_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"project"));
    }

    #[test]
    fn view_query_replace() {
        use crate::table::types::ViewInputs;
        let mut old = base();
        old.view = Some(ViewInputs {
            query: "SELECT 1",
            use_legacy_sql: None,
        });
        let mut new = base();
        new.view = Some(ViewInputs {
            query: "SELECT 2",
            use_legacy_sql: None,
        });
        let diff = compute_table_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"view.query"));
    }

    #[test]
    fn labels_update() {
        let old = base();
        let mut new = base();
        new.labels.insert("env", "prod");
        let diff = compute_table_diff(&old, &new);
        assert!(diff.has_changes());
        assert!(!diff.needs_replace());
        assert!(diff.update_keys.contains(&"labels"));
    }

    #[test]
    fn external_data_config_replace() {
        use crate::table::types::ExternalDataConfig;
        let mut old = base();
        old.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/a.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: None,
            csv_skip_leading_rows: None,
        });
        let mut new = base();
        new.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/b.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: None,
            csv_skip_leading_rows: None,
        });
        let diff = compute_table_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"externalDataConfiguration"));
    }

    #[test]
    fn external_data_config_added_replace() {
        use crate::table::types::ExternalDataConfig;
        let old = base();
        let mut new = base();
        new.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/a.csv"],
            source_format: "CSV",
            autodetect: None,
            connection_id: None,
            csv_skip_leading_rows: None,
        });
        let diff = compute_table_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"externalDataConfiguration"));
    }

    #[test]
    fn external_data_config_no_change() {
        use crate::table::types::ExternalDataConfig;
        let mut old = base();
        old.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/a.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: None,
            csv_skip_leading_rows: Some(1),
        });
        let mut new = base();
        new.external_data_config = Some(ExternalDataConfig {
            source_uris: vec!["gs://bucket/a.csv"],
            source_format: "CSV",
            autodetect: Some(true),
            connection_id: None,
            csv_skip_leading_rows: Some(1),
        });
        let diff = compute_table_diff(&old, &new);
        assert!(!diff.has_changes());
    }
}
