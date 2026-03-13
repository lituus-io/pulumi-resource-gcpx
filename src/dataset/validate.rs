use crate::dataset::types::DatasetInputs;
use crate::schema::types::CheckFailure;

pub fn validate_dataset(inputs: &DatasetInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if inputs.project.is_empty() {
        failures.push(CheckFailure {
            property: "project".into(),
            reason: "project cannot be empty: provide a valid GCP project ID".into(),
        });
    }
    if inputs.dataset_id.is_empty() {
        failures.push(CheckFailure {
            property: "datasetId".into(),
            reason: "datasetId cannot be empty: must be alphanumeric/underscores, 1-1024 chars"
                .into(),
        });
    }
    if inputs.location.is_empty() {
        failures.push(CheckFailure {
            property: "location".into(),
            reason: "location cannot be empty: use 'US', 'EU', or a regional location (e.g., 'us-central1')".into(),
        });
    }

    if let Some(sbm) = inputs.storage_billing_model {
        if !matches!(sbm, "LOGICAL" | "PHYSICAL") {
            failures.push(CheckFailure {
                property: "storageBillingModel".into(),
                reason: "storageBillingModel must be 'LOGICAL' or 'PHYSICAL' (case-sensitive)"
                    .into(),
            });
        }
    }

    if let Some(hours) = inputs.max_time_travel_hours {
        if !(48..=168).contains(&hours) && hours != 0 {
            failures.push(CheckFailure {
                property: "maxTimeTravelHours".into(),
                reason: "maxTimeTravelHours must be 0 (disabled) or between 48-168 hours (2-7 day recovery window)".into(),
            });
        }
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base<'a>() -> DatasetInputs<'a> {
        DatasetInputs {
            project: "proj",
            dataset_id: "ds",
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
    fn valid_dataset() {
        assert!(validate_dataset(&base()).is_empty());
    }

    #[test]
    fn empty_project() {
        let mut inputs = base();
        inputs.project = "";
        let failures = validate_dataset(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("project cannot be empty")));
    }

    #[test]
    fn empty_location() {
        let mut inputs = base();
        inputs.location = "";
        let failures = validate_dataset(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("location cannot be empty")));
    }

    #[test]
    fn invalid_storage_billing_model() {
        let mut inputs = base();
        inputs.storage_billing_model = Some("INVALID");
        let failures = validate_dataset(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("'LOGICAL' or 'PHYSICAL'")));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn validate_dataset_never_panics(
            project in "[ -~]{0,30}",
            dataset_id in "[ -~]{0,30}",
            location in "[ -~]{0,20}",
            hours in 0i64..=200i64,
        ) {
            let inputs = DatasetInputs {
                project: &project,
                dataset_id: &dataset_id,
                location: &location,
                description: None,
                friendly_name: None,
                labels: BTreeMap::new(),
                default_table_expiration_ms: None,
                default_partition_expiration_ms: None,
                storage_billing_model: None,
                max_time_travel_hours: Some(hours),
            };
            let _ = std::hint::black_box(validate_dataset(&inputs));
        }
    }
}
