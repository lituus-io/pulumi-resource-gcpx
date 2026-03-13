#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::dbt::scanner::DbtScanner;

fuzz_target!(|data: &str| {
    let scanner = DbtScanner::new(data);
    for segment in scanner {
        let _ = std::hint::black_box(segment);
    }
});
