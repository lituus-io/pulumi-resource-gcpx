#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

fuzz_target!(|data: &str| {
    let yaml = generate_dataproc_workflow_yaml(
        data, data, data,
        data, data, &[], &[data],
        "2.2", data, data, data, &[], false,
    );
    assert!(yaml.contains("dataproc.googleapis.com"));
    assert!(yaml.contains("batchId"));
});
