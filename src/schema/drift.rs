//! Schema drift detection — compares declared SchemaField definitions
//! against live BigQuery field metadata. True zero-copy: all string
//! references borrow from the input slices.

use crate::bq::BqField;
use crate::schema::types::{normalize_type, SchemaField};

/// Report of schema drift between declared and live BigQuery state.
pub struct DriftReport<'a> {
    /// Fields present in BigQuery but not declared in the Pulumi schema.
    pub extra_in_bq: Vec<&'a str>,
    /// Fields declared in the Pulumi schema but missing from BigQuery.
    pub missing_from_bq: Vec<&'a str>,
    /// Fields where the declared type differs from the live type.
    /// Tuple: (field_name, declared_canonical_type, actual_bq_type).
    pub type_mismatches: Vec<(&'a str, &'a str, &'a str)>,
}

impl DriftReport<'_> {
    /// Returns `true` when declared and live schemas are in sync.
    pub fn is_clean(&self) -> bool {
        self.extra_in_bq.is_empty()
            && self.missing_from_bq.is_empty()
            && self.type_mismatches.is_empty()
    }
}

/// Detect drift between declared schema fields and live BigQuery fields.
///
/// `declared` borrows from the Pulumi input; `live` borrows from the BQ API
/// response. The report borrows field names from both, so the caller must
/// keep both slices alive while using the report.
pub fn detect_drift<'a>(declared: &'a [SchemaField<'_>], live: &'a [BqField]) -> DriftReport<'a> {
    let mut extra_in_bq: Vec<&'a str> = Vec::new();
    let mut missing_from_bq: Vec<&'a str> = Vec::new();
    let mut type_mismatches: Vec<(&'a str, &'a str, &'a str)> = Vec::new();

    // Check each declared field against live.
    for df in declared {
        match live.iter().find(|lf| lf.name.eq_ignore_ascii_case(df.name)) {
            Some(lf) => {
                let live_canonical = normalize_type(&lf.field_type);
                if df.canonical_type != live_canonical {
                    type_mismatches.push((df.name, df.canonical_type, &lf.field_type));
                }
            }
            None => {
                missing_from_bq.push(df.name);
            }
        }
    }

    // Check each live field against declared.
    for lf in live {
        let found = declared
            .iter()
            .any(|df| df.name.eq_ignore_ascii_case(&lf.name));
        if !found {
            extra_in_bq.push(&lf.name);
        }
    }

    DriftReport {
        extra_in_bq,
        missing_from_bq,
        type_mismatches,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bq_field(name: &str, field_type: &str) -> BqField {
        BqField {
            name: name.to_owned(),
            field_type: field_type.to_owned(),
            mode: "NULLABLE".to_owned(),
            description: String::new(),
            fields: vec![],
        }
    }

    fn schema_field(name: &str, raw_type: &str) -> SchemaField<'static> {
        // Leak for test convenience — static lifetime.
        let name: &'static str = Box::leak(name.to_owned().into_boxed_str());
        let raw_type: &'static str = Box::leak(raw_type.to_owned().into_boxed_str());
        SchemaField {
            name,
            raw_type,
            canonical_type: normalize_type(raw_type),
            mode: "NULLABLE",
            description: "",
            alter: None,
            alter_raw: None,
            alter_from: None,
            default_value_expression: None,
            rounding_mode: None,
            fields: vec![],
        }
    }

    #[test]
    fn no_drift() {
        let declared = [
            schema_field("col_a", "STRING"),
            schema_field("col_b", "INT64"),
        ];
        let live = [bq_field("col_a", "STRING"), bq_field("col_b", "INT64")];
        let report = detect_drift(&declared, &live);
        assert!(report.is_clean());
    }

    #[test]
    fn extra_in_bq() {
        let declared = [schema_field("col_a", "STRING")];
        let live = [bq_field("col_a", "STRING"), bq_field("col_b", "INT64")];
        let report = detect_drift(&declared, &live);
        assert_eq!(report.extra_in_bq, vec!["col_b"]);
        assert!(report.missing_from_bq.is_empty());
    }

    #[test]
    fn missing_from_bq() {
        let declared = [
            schema_field("col_a", "STRING"),
            schema_field("col_b", "INT64"),
        ];
        let live = [bq_field("col_a", "STRING")];
        let report = detect_drift(&declared, &live);
        assert!(report.extra_in_bq.is_empty());
        assert_eq!(report.missing_from_bq, vec!["col_b"]);
    }

    #[test]
    fn type_mismatch() {
        let declared = [schema_field("col_a", "STRING")];
        let live = [bq_field("col_a", "INT64")];
        let report = detect_drift(&declared, &live);
        assert!(report.extra_in_bq.is_empty());
        assert!(report.missing_from_bq.is_empty());
        assert_eq!(report.type_mismatches.len(), 1);
        assert_eq!(report.type_mismatches[0], ("col_a", "STRING", "INT64"));
    }

    #[test]
    fn case_insensitive_match() {
        let declared = [schema_field("Col_A", "STRING")];
        let live = [bq_field("col_a", "STRING")];
        let report = detect_drift(&declared, &live);
        assert!(report.is_clean());
    }
}
