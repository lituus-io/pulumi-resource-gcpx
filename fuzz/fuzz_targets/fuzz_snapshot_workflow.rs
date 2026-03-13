#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::snapshot::types::SnapshotInputs;
use pulumi_resource_gcpx::snapshot::workflow::generate_snapshot_workflow_yaml;

fuzz_target!(|data: &str| {
    let inputs = SnapshotInputs {
        project: "proj",
        region: "us-central1",
        dataset: "ds",
        name: "snap",
        source_sql: data,
        unique_key: "id",
        strategy: "timestamp",
        updated_at: "updated_at",
        schedule: "0 2 * * *",
        time_zone: "UTC",
        service_account: "sa@iam",
        description: None,
        paused: None,
        invalidate_hard_deletes: Some(true),
        auto_optimize: None,
        source_schema: None,
    };
    let yaml = generate_snapshot_workflow_yaml(&inputs);
    assert!(yaml.contains("invalidateChanged:"));
    assert!(yaml.contains("insertNewVersions:"));
    assert!(yaml.contains("invalidateHardDeletes:"));
});
