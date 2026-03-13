use std::borrow::Cow;

use crate::schema::types::CheckFailure;

/// Unified diff result replacing DatasetDiffResult, TableDiffResult, RoutineDiffResult.
pub struct DiffResult {
    pub replace_keys: Vec<&'static str>,
    pub update_keys: Vec<&'static str>,
}

impl DiffResult {
    pub fn has_changes(&self) -> bool {
        !self.replace_keys.is_empty() || !self.update_keys.is_empty()
    }

    pub fn needs_replace(&self) -> bool {
        !self.replace_keys.is_empty()
    }
}

/// Validation helper — pushes a CheckFailure if `value` is empty.
pub fn require_non_empty(failures: &mut Vec<CheckFailure>, property: &'static str, value: &str) {
    if value.is_empty() {
        failures.push(CheckFailure {
            property: Cow::Borrowed(property),
            reason: Cow::Owned(format!("{property} must not be empty")),
        });
    }
}
