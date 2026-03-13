#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::dbt::scanner::ConfigArgIter;

fuzz_target!(|data: &str| {
    let iter = ConfigArgIter::new(data);
    for entry in iter {
        let _ = std::hint::black_box(entry.key);
    }
});
