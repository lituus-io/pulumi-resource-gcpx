#![no_main]
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use pulumi_resource_gcpx::dbt::macros::{expand_macros, MacroDef};

fuzz_target!(|data: &str| {
    // Create a simple macro environment
    let mut macros = BTreeMap::new();
    macros.insert(
        "test".to_owned(),
        MacroDef {
            args: vec!["x".to_owned()],
            sql: "RESULT".to_owned(),
        },
    );
    // Must never panic
    let _ = expand_macros(data, &macros);
});
