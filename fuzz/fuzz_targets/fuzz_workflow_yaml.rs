#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml;

fuzz_target!(|data: &str| {
    let yaml = generate_workflow_yaml("test-project", data);
    // Basic validity: must contain the BQ API call
    assert!(yaml.contains("googleapis.bigquery.v2.jobs.query"));
});
