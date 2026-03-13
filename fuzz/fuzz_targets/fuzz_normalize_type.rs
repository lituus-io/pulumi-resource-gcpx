#![no_main]
use libfuzzer_sys::fuzz_target;
use pulumi_resource_gcpx::schema::types::normalize_type;

fuzz_target!(|data: &str| {
    let result = normalize_type(data);
    // Idempotency: normalize(normalize(x)) == normalize(x)
    let result2 = normalize_type(result);
    assert_eq!(result, result2);
});
