use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

fn bench_dataproc_workflow_arg_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataproc_workflow_args");

    for count in [0, 5, 10, 20] {
        let args: Vec<&str> = (0..count).map(|_| "arg_value").collect();
        group.bench_with_input(BenchmarkId::new("generate", count), &args, |b, args| {
            b.iter(|| {
                black_box(generate_dataproc_workflow_yaml(
                    black_box("project"),
                    black_box("us-central1"),
                    black_box("test-job"),
                    black_box("gcr.io/proj/spark:latest"),
                    black_box("gs://bucket/main.py"),
                    black_box(&[]),
                    black_box(args),
                    black_box("2.2"),
                    black_box("sa@iam"),
                    black_box("staging"),
                    black_box("subnet"),
                    black_box(&["spark"]),
                    black_box(false),
                ));
            })
        });
    }
    group.finish();
}

fn bench_dataproc_workflow_query_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataproc_workflow_query");

    for size in [256, 1024, 4096] {
        let query = "X".repeat(size);
        let query_ref = query.as_str();
        group.bench_with_input(BenchmarkId::new("generate", size), &query_ref, |b, q| {
            b.iter(|| {
                black_box(generate_dataproc_workflow_yaml(
                    black_box("project"),
                    black_box("us-central1"),
                    black_box("test-job"),
                    black_box("gcr.io/proj/spark:latest"),
                    black_box("gs://bucket/main.py"),
                    black_box(&[]),
                    black_box(&[q]),
                    black_box("2.2"),
                    black_box("sa@iam"),
                    black_box("staging"),
                    black_box("subnet"),
                    black_box(&["spark"]),
                    black_box(false),
                ));
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_dataproc_workflow_arg_counts,
    bench_dataproc_workflow_query_sizes,
);
criterion_main!(benches);
