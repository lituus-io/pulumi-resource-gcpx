use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pulumi_resource_gcpx::dbt::macros::expand_macros;
use pulumi_resource_gcpx::dbt::macros::MacroDef;
use pulumi_resource_gcpx::dbt::resolver::resolve;
use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

fn make_sources() -> BTreeMap<String, SourceDef> {
    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: "raw_data".to_owned(),
            tables: vec!["customers".to_owned(), "orders".to_owned()],
        },
    );
    sources
}

fn make_ref(name: &str, materialization: &str) -> (String, ModelRefData) {
    (
        name.to_owned(),
        ModelRefData {
            materialization: materialization.to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: format!("SELECT * FROM raw_{}", name),
            table_ref: format!("`p.d.{}`", name),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    )
}

#[allow(clippy::type_complexity)]
fn build_chain(
    depth: usize,
) -> (
    String,
    BTreeMap<String, SourceDef>,
    BTreeMap<String, ModelRefData>,
    BTreeMap<String, MacroDef>,
) {
    let sources = make_sources();
    let mut refs = BTreeMap::new();
    let macros = BTreeMap::new();

    // Build a chain of ephemeral refs: model_0 -> model_1 -> ... -> model_{depth-1}
    for i in 0..depth {
        let name = format!("model_{}", i);
        let _body = if i == 0 {
            "SELECT * FROM {{ source('raw', 'customers') }}".to_owned()
        } else {
            format!("SELECT * FROM {{{{ ref('model_{}') }}}}", i - 1)
        };
        refs.insert(
            name.clone(),
            ModelRefData {
                materialization: "ephemeral".to_owned(),
                resolved_ctes_json: "[]".to_owned(),
                resolved_body: format!("SELECT * FROM upstream_{}", i),
                table_ref: String::new(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
    }

    let sql = format!(
        "{{{{ config(materialized='table') }}}}\nSELECT * FROM {{{{ ref('model_{}') }}}}",
        depth - 1
    );

    (sql, sources, refs, macros)
}

fn bench_resolve_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbt_resolver");

    for depth in [1, 3, 5, 10] {
        let (sql, sources, refs, macros) = build_chain(depth);
        group.bench_with_input(
            BenchmarkId::new("ephemeral_chain", depth),
            &(sql, sources, refs, macros),
            |b, (sql, sources, refs, macros)| {
                b.iter(|| {
                    black_box(resolve(black_box(sql), "p", "d", sources, refs, macros).unwrap());
                })
            },
        );
    }
    group.finish();
}

fn bench_macro_expansion(c: &mut Criterion) {
    let mut macros = BTreeMap::new();
    for i in 0..5 {
        macros.insert(
            format!("macro_{}", i),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: format!("FUNC_{}({{{{ x }}}})", i),
            },
        );
    }

    let mut sql = String::from("SELECT ");
    for i in 0..20 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("{{{{ macro_{}('col_{}') }}}}", i % 5, i));
    }
    sql.push_str(" FROM t");

    c.bench_function("expand_20_macros", |b| {
        b.iter(|| {
            black_box(expand_macros(black_box(&sql), black_box(&macros)).unwrap());
        })
    });
}

fn bench_resolve_with_sources(c: &mut Criterion) {
    let sources = make_sources();
    let refs: BTreeMap<String, ModelRefData> = vec![make_ref("stg", "view")].into_iter().collect();
    let macros = BTreeMap::new();

    let sql = "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('stg') }} JOIN {{ source('raw', 'customers') }}";

    c.bench_function("resolve_ref_and_source", |b| {
        b.iter(|| {
            black_box(resolve(black_box(sql), "p", "d", &sources, &refs, &macros).unwrap());
        })
    });
}

criterion_group!(
    benches,
    bench_resolve_chain,
    bench_macro_expansion,
    bench_resolve_with_sources
);
criterion_main!(benches);
