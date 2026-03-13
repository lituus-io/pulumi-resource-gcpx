use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pulumi_resource_gcpx::dbt::scanner::{ConfigArgIter, DbtScanner};

fn generate_dbt_sql(approx_size: usize) -> String {
    let mut sql = String::with_capacity(approx_size + 256);
    sql.push_str("{{ config(materialized='table', tags=['daily']) }}\n");
    sql.push_str("WITH\n");

    let base_cte = "cte_x AS (\n  SELECT id, name, email\n  FROM {{ source('raw', 'customers') }}\n  WHERE active = true\n),\n";
    let ref_line = "joined AS (\n  SELECT * FROM {{ ref('stg_customers') }}\n),\n";

    while sql.len() < approx_size {
        sql.push_str(base_cte);
        sql.push_str(ref_line);
        sql.push_str("SELECT {{ surrogate_key('id, email') }} FROM t\n");
    }
    sql.push_str("SELECT * FROM joined");
    sql
}

fn bench_scanner_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbt_scanner");

    for size in [256, 1024, 4096, 16384] {
        let sql = generate_dbt_sql(size);
        group.bench_with_input(BenchmarkId::new("scan", size), &sql, |b, sql| {
            b.iter(|| {
                let scanner = DbtScanner::new(black_box(sql));
                for seg in scanner {
                    black_box(seg);
                }
            })
        });
    }
    group.finish();
}

fn bench_config_parse(c: &mut Criterion) {
    let configs = [
        "materialized='table'",
        "materialized='table', tags=['daily', 'mart']",
        "materialized='incremental', unique_key='id', strategy='merge', tags=['daily']",
    ];
    let mut group = c.benchmark_group("config_parse");
    for (i, config) in configs.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("parse", i), config, |b, cfg| {
            b.iter(|| {
                let iter = ConfigArgIter::new(black_box(cfg));
                for entry in iter {
                    black_box(entry);
                }
            })
        });
    }
    group.finish();
}

fn bench_normalize_type(c: &mut Criterion) {
    let types = [
        "STRING",
        "INT64",
        "FLOAT64",
        "BOOLEAN",
        "TIMESTAMP",
        "BIGNUMERIC",
        "RECORD",
        "TINYINT",
        "BADTYPE",
    ];
    c.bench_function("normalize_type_batch", |b| {
        b.iter(|| {
            for ty in &types {
                black_box(pulumi_resource_gcpx::schema::types::normalize_type(
                    black_box(ty),
                ));
            }
        })
    });
}

fn bench_workflow_yaml(c: &mut Criterion) {
    let sql =
        "CREATE OR REPLACE TABLE `p.d.t` AS WITH cte AS (SELECT * FROM source) SELECT * FROM cte";
    c.bench_function("workflow_yaml_generation", |b| {
        b.iter(|| {
            black_box(
                pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml(
                    black_box("project"),
                    black_box(sql),
                ),
            );
        })
    });
}

criterion_group!(
    benches,
    bench_scanner_sizes,
    bench_config_parse,
    bench_normalize_type,
    bench_workflow_yaml,
);
criterion_main!(benches);
