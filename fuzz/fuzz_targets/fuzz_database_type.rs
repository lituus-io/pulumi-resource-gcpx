#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::dataproc::types::DatabaseType;

fuzz_target!(|data: &str| {
    // Must never panic — only returns Some or None
    let _ = DatabaseType::parse(data);
});
