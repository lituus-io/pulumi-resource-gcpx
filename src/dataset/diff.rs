use crate::dataset::types::DatasetInputs;
use crate::resource::DiffResult;

pub fn compute_dataset_diff(old: &DatasetInputs<'_>, new: &DatasetInputs<'_>) -> DiffResult {
    let mut replace_keys = Vec::new();
    let mut update_keys = Vec::new();

    // Replace triggers.
    if old.project != new.project {
        replace_keys.push("project");
    }
    if old.dataset_id != new.dataset_id {
        replace_keys.push("datasetId");
    }
    if old.location != new.location {
        replace_keys.push("location");
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
    if old.default_table_expiration_ms != new.default_table_expiration_ms {
        update_keys.push("defaultTableExpirationMs");
    }
    if old.default_partition_expiration_ms != new.default_partition_expiration_ms {
        update_keys.push("defaultPartitionExpirationMs");
    }
    if old.storage_billing_model != new.storage_billing_model {
        update_keys.push("storageBillingModel");
    }
    if old.max_time_travel_hours != new.max_time_travel_hours {
        update_keys.push("maxTimeTravelHours");
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
    fn no_changes() {
        let diff = compute_dataset_diff(&base(), &base());
        assert!(!diff.has_changes());
    }

    #[test]
    fn location_replaces() {
        let old = base();
        let mut new = base();
        new.location = "EU";
        let diff = compute_dataset_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"location"));
    }

    #[test]
    fn description_updates() {
        let old = base();
        let mut new = base();
        new.description = Some("new");
        let diff = compute_dataset_diff(&old, &new);
        assert!(diff.has_changes());
        assert!(!diff.needs_replace());
        assert!(diff.update_keys.contains(&"description"));
    }

    #[test]
    fn labels_update() {
        let old = base();
        let mut new = base();
        new.labels.insert("env", "prod");
        let diff = compute_dataset_diff(&old, &new);
        assert!(diff.has_changes());
        assert!(!diff.needs_replace());
        assert!(diff.update_keys.contains(&"labels"));
    }
}
