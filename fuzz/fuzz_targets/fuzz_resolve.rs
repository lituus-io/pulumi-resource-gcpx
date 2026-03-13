#![no_main]
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use pulumi_resource_gcpx::dbt::macros::MacroDef;
use pulumi_resource_gcpx::dbt::resolver::resolve;
use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

fuzz_target!(|data: &str| {
    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: "raw_data".to_owned(),
            tables: vec!["t".to_owned()],
        },
    );

    let mut refs = BTreeMap::new();
    refs.insert(
        "stg".to_owned(),
        ModelRefData {
            materialization: "view".to_owned(),
            resolved_ctes_json: String::new(),
            resolved_body: String::new(),
            table_ref: "`p.d.stg`".to_owned(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );

    let macros = BTreeMap::new();
    let _ = resolve(data, "project", "dataset", &sources, &refs, &macros);
});
