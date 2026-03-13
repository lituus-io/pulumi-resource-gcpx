use std::collections::HashMap;

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::schema::types::CheckFailure;

/// Build a CheckResponse from news and validation failures.
/// Eliminates the repeated CheckFailure → pulumirpc::CheckFailure mapping
/// across all resource check handlers.
#[allow(clippy::result_large_err)]
pub fn build_check_response(
    news: Option<prost_types::Struct>,
    failures: Vec<CheckFailure>,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let grpc_failures = failures
        .into_iter()
        .map(|f| pulumirpc::CheckFailure {
            property: f.property.into_owned(),
            reason: f.reason.into_owned(),
        })
        .collect();

    Ok(Response::new(pulumirpc::CheckResponse {
        inputs: news,
        failures: grpc_failures,
    }))
}

/// Build a DiffResponse from replace and update key lists.
/// Eliminates the repeated detailed_diff HashMap construction
/// across all resource diff handlers.
pub fn build_diff_response(
    replace_keys: &[&str],
    update_keys: &[&str],
) -> Response<pulumirpc::DiffResponse> {
    let has_changes = !replace_keys.is_empty() || !update_keys.is_empty();
    let changes = if has_changes {
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    } else {
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    };

    let replaces: Vec<String> = replace_keys.iter().map(|s| s.to_string()).collect();

    let mut detailed_diff = HashMap::with_capacity(replace_keys.len() + update_keys.len());
    for key in replace_keys {
        detailed_diff.insert(
            key.to_string(),
            pulumirpc::PropertyDiff {
                kind: pulumirpc::property_diff::Kind::UpdateReplace as i32,
                input_diff: true,
            },
        );
    }
    for key in update_keys {
        detailed_diff.insert(
            key.to_string(),
            pulumirpc::PropertyDiff {
                kind: pulumirpc::property_diff::Kind::Update as i32,
                input_diff: true,
            },
        );
    }

    Response::new(pulumirpc::DiffResponse {
        changes,
        replaces,
        has_detailed_diff: true,
        detailed_diff,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_response_no_failures() {
        let resp = build_check_response(None, vec![]).unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[test]
    fn check_response_with_failures() {
        let failures = vec![CheckFailure {
            property: "name".into(),
            reason: "name must not be empty".into(),
        }];
        let resp = build_check_response(None, failures).unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.failures.len(), 1);
        assert_eq!(inner.failures[0].property, "name");
    }

    #[test]
    fn diff_response_no_changes() {
        let resp = build_diff_response(&[], &[]);
        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        );
        assert!(inner.replaces.is_empty());
        assert!(inner.detailed_diff.is_empty());
    }

    #[test]
    fn diff_response_with_replaces() {
        let resp = build_diff_response(&["project", "dataset"], &[]);
        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
        assert_eq!(inner.replaces.len(), 2);
        assert!(inner.detailed_diff.contains_key("project"));
        assert_eq!(
            inner.detailed_diff["project"].kind,
            pulumirpc::property_diff::Kind::UpdateReplace as i32
        );
    }

    #[test]
    fn diff_response_with_updates() {
        let resp = build_diff_response(&[], &["description", "labels"]);
        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
        assert!(inner.replaces.is_empty());
        assert!(inner.detailed_diff.contains_key("description"));
        assert_eq!(
            inner.detailed_diff["description"].kind,
            pulumirpc::property_diff::Kind::Update as i32
        );
    }

    #[test]
    fn diff_response_mixed() {
        let resp = build_diff_response(&["project"], &["description"]);
        let inner = resp.into_inner();
        assert_eq!(inner.replaces.len(), 1);
        assert_eq!(inner.detailed_diff.len(), 2);
        assert_eq!(
            inner.detailed_diff["project"].kind,
            pulumirpc::property_diff::Kind::UpdateReplace as i32
        );
        assert_eq!(
            inner.detailed_diff["description"].kind,
            pulumirpc::property_diff::Kind::Update as i32
        );
    }
}
