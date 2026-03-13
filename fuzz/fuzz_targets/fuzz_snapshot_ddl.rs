#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::snapshot::ddl::{
    generate_hard_delete_sql, generate_insert_sql, generate_invalidate_sql,
    generate_snapshot_create_ddl,
};
use pulumi_resource_gcpx::snapshot::types::SnapshotInputs;

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
    let _ = std::hint::black_box(generate_snapshot_create_ddl(&inputs));
    let _ = std::hint::black_box(generate_invalidate_sql(&inputs));
    let _ = std::hint::black_box(generate_insert_sql(&inputs));
    let _ = std::hint::black_box(generate_hard_delete_sql(&inputs));
});
