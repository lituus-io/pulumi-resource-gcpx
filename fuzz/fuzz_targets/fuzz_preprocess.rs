#![no_main]
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use pulumi_resource_gcpx::dbt::preprocess::{
    expand_builtins, expand_vars, handle_incremental_blocks, preprocess, replace_this,
};

fuzz_target!(|data: &str| {
    let mut vars = BTreeMap::new();
    vars.insert("days".to_owned(), "90".to_owned());
    vars.insert("threshold".to_owned(), "100".to_owned());

    let _ = handle_incremental_blocks(data, true);
    let _ = handle_incremental_blocks(data, false);
    let _ = replace_this(data, "`p.d.t`");
    let _ = expand_vars(data, &vars);
    let _ = expand_builtins(data);
    let _ = preprocess(data, &vars, "`p.d.t`", false);
    let _ = preprocess(data, &vars, "`p.d.t`", true);
});
