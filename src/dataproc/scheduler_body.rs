//! Shared Cloud Scheduler JSON body construction for workflow-backed resources.

use base64::Engine;

/// Configuration for building a Cloud Scheduler job body.
pub struct SchedulerBodyConfig<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub sched_name: &'a str,
    pub wf_name: &'a str,
    pub schedule: &'a str,
    pub time_zone: &'a str,
    pub service_account: &'a str,
    pub description: Option<&'a str>,
    pub paused: Option<bool>,
    pub retry_count: Option<i32>,
    pub attempt_deadline: Option<&'a str>,
}

fn workflow_uri(project: &str, region: &str, wf_name: &str) -> String {
    format!(
        "https://workflowexecutions.googleapis.com/v1/projects/{}/locations/{}/workflows/{}/executions",
        project, region, wf_name,
    )
}

fn http_target(uri: &str, service_account: &str) -> serde_json::Value {
    serde_json::json!({
        "uri": uri,
        "httpMethod": "POST",
        "body": base64::engine::general_purpose::STANDARD.encode(b"{}"),
        "oauthToken": {
            "serviceAccountEmail": service_account,
            "scope": "https://www.googleapis.com/auth/cloud-platform",
        },
    })
}

/// Build the full create body for a Cloud Scheduler job that triggers a workflow.
pub fn build_scheduler_create_body(cfg: &SchedulerBodyConfig<'_>) -> serde_json::Value {
    let uri = workflow_uri(cfg.project, cfg.region, cfg.wf_name);

    let mut body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", cfg.project, cfg.region, cfg.sched_name),
        "schedule": cfg.schedule,
        "timeZone": cfg.time_zone,
        "httpTarget": http_target(&uri, cfg.service_account),
    });

    apply_optional_fields(&mut body, cfg);

    let retry_count = cfg.retry_count.unwrap_or(3);
    let deadline = cfg.attempt_deadline.unwrap_or("600s");
    body["retryConfig"] = serde_json::json!({ "retryCount": retry_count });
    body["attemptDeadline"] = serde_json::Value::String(deadline.to_owned());

    body
}

/// Build a patch body for updating a Cloud Scheduler job.
///
/// `include_retry` — set to false for snapshot (which doesn't patch retry fields).
///
/// State (paused/enabled) is NOT included — the Cloud Scheduler API ignores
/// `state` in PATCH bodies. Use the dedicated pause/resume endpoints instead.
pub fn build_scheduler_patch_body(
    cfg: &SchedulerBodyConfig<'_>,
    include_retry: bool,
) -> serde_json::Value {
    let uri = workflow_uri(cfg.project, cfg.region, cfg.wf_name);

    let mut body = serde_json::json!({
        "schedule": cfg.schedule,
        "timeZone": cfg.time_zone,
        "httpTarget": http_target(&uri, cfg.service_account),
    });

    if let Some(desc) = cfg.description {
        body["description"] = serde_json::Value::String(desc.to_owned());
    }
    if include_retry {
        let retry_count = cfg.retry_count.unwrap_or(3);
        let deadline = cfg.attempt_deadline.unwrap_or("600s");
        body["retryConfig"] = serde_json::json!({ "retryCount": retry_count });
        body["attemptDeadline"] = serde_json::Value::String(deadline.to_owned());
    }

    body
}

fn apply_optional_fields(body: &mut serde_json::Value, cfg: &SchedulerBodyConfig<'_>) {
    if let Some(desc) = cfg.description {
        body["description"] = serde_json::Value::String(desc.to_owned());
    }
    // Note: state is NOT set here — the Cloud Scheduler API ignores state in
    // create/patch bodies. Use the dedicated pause/resume endpoints instead.
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> SchedulerBodyConfig<'static> {
        SchedulerBodyConfig {
            project: "proj",
            region: "us-central1",
            sched_name: "gcpx-sched-test",
            wf_name: "gcpx-wf-test",
            schedule: "0 * * * *",
            time_zone: "UTC",
            service_account: "sa@proj.iam.gserviceaccount.com",
            description: None,
            paused: None,
            retry_count: None,
            attempt_deadline: None,
        }
    }

    #[test]
    fn create_body_has_required_fields() {
        let body = build_scheduler_create_body(&test_cfg());
        assert!(body["name"].as_str().unwrap().contains("gcpx-sched-test"));
        assert_eq!(body["schedule"], "0 * * * *");
        assert_eq!(body["timeZone"], "UTC");
        assert_eq!(body["retryConfig"]["retryCount"], 3);
        assert_eq!(body["attemptDeadline"], "600s");
        assert!(body["httpTarget"]["uri"]
            .as_str()
            .unwrap()
            .contains("gcpx-wf-test"));
    }

    #[test]
    fn create_body_with_paused_does_not_set_state() {
        let cfg = SchedulerBodyConfig {
            paused: Some(true),
            ..test_cfg()
        };
        let body = build_scheduler_create_body(&cfg);
        // State is NOT set in create body — use dedicated pause API instead.
        assert!(body.get("state").is_none());
    }

    #[test]
    fn create_body_with_description() {
        let cfg = SchedulerBodyConfig {
            description: Some("test desc"),
            ..test_cfg()
        };
        let body = build_scheduler_create_body(&cfg);
        assert_eq!(body["description"], "test desc");
    }

    #[test]
    fn create_body_custom_retry() {
        let cfg = SchedulerBodyConfig {
            retry_count: Some(5),
            attempt_deadline: Some("300s"),
            ..test_cfg()
        };
        let body = build_scheduler_create_body(&cfg);
        assert_eq!(body["retryConfig"]["retryCount"], 5);
        assert_eq!(body["attemptDeadline"], "300s");
    }

    #[test]
    fn patch_body_never_includes_state() {
        let cfg = SchedulerBodyConfig {
            paused: Some(true),
            ..test_cfg()
        };
        let body = build_scheduler_patch_body(&cfg, true);
        // State is never set in patch body — use dedicated pause/resume API.
        assert!(body.get("state").is_none());
        assert_eq!(body["retryConfig"]["retryCount"], 3);
    }

    #[test]
    fn patch_body_without_retry() {
        let body = build_scheduler_patch_body(&test_cfg(), false);
        assert!(body.get("retryConfig").is_none());
        assert!(body.get("attemptDeadline").is_none());
    }
}
