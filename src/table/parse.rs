use std::collections::BTreeMap;

use crate::output::OutputBuilder;
use crate::prost_util::{get_bool, get_list, get_number, get_str, get_struct_fields, value_as_str};
use crate::table::types::{
    ExternalDataConfig, MaterializedViewInputs, RangePartitioningInputs, TableInputs,
    TimePartitioningInputs, ViewInputs,
};

pub fn parse_table_inputs(s: &prost_types::Struct) -> Result<TableInputs<'_>, &'static str> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID (e.g., 'my-project')")?;
    let dataset = get_str(&s.fields, "dataset").ok_or(
        "missing required field 'dataset': provide a BigQuery dataset ID (e.g., 'analytics')",
    )?;
    let table_id = get_str(&s.fields, "tableId").ok_or(
        "missing required field 'tableId': provide a BigQuery table name (e.g., 'customers')",
    )?;
    let description = get_str(&s.fields, "description");
    let friendly_name = get_str(&s.fields, "friendlyName");
    let deletion_protection = get_bool(&s.fields, "deletionProtection");
    let expiration_time = get_number(&s.fields, "expirationTime").map(|n| n as i64);
    let encryption_kms_key = get_str(&s.fields, "encryptionKmsKey");
    let storage_billing_model = get_str(&s.fields, "storageBillingModel");
    let max_staleness = get_str(&s.fields, "maxStaleness");

    let mut labels = BTreeMap::new();
    if let Some(lf) = get_struct_fields(&s.fields, "labels") {
        for (k, v) in lf {
            if let Some(val) = value_as_str(v) {
                labels.insert(k.as_str(), val);
            }
        }
    }

    let view = get_struct_fields(&s.fields, "view").map(|vf| ViewInputs {
        query: get_str(vf, "query").unwrap_or(""),
        use_legacy_sql: get_bool(vf, "useLegacySql"),
    });

    let materialized_view =
        get_struct_fields(&s.fields, "materializedView").map(|mvf| MaterializedViewInputs {
            query: get_str(mvf, "query").unwrap_or(""),
            enable_refresh: get_bool(mvf, "enableRefresh"),
            refresh_interval_ms: get_number(mvf, "refreshIntervalMs").map(|n| n as i64),
            max_staleness: get_str(mvf, "maxStaleness"),
        });

    let time_partitioning =
        get_struct_fields(&s.fields, "timePartitioning").map(|tpf| TimePartitioningInputs {
            partition_type: get_str(tpf, "type").unwrap_or("DAY"),
            field: get_str(tpf, "field"),
            expiration_ms: get_number(tpf, "expirationMs").map(|n| n as i64),
        });

    let range_partitioning = get_struct_fields(&s.fields, "rangePartitioning").and_then(|rpf| {
        let field = get_str(rpf, "field")?;
        let range = get_struct_fields(rpf, "range")?;
        let start = get_number(range, "start")? as i64;
        let end = get_number(range, "end")? as i64;
        let interval = get_number(range, "interval")? as i64;
        Some(RangePartitioningInputs {
            field,
            start,
            end,
            interval,
        })
    });

    let clusterings = get_list(&s.fields, "clusterings")
        .map(|items| items.iter().filter_map(value_as_str).collect::<Vec<_>>())
        .unwrap_or_default();

    let external_data_config =
        get_struct_fields(&s.fields, "externalDataConfiguration").map(|edc| {
            let source_uris = get_list(edc, "sourceUris")
                .map(|items| items.iter().filter_map(value_as_str).collect::<Vec<_>>())
                .unwrap_or_default();
            let source_format = get_str(edc, "sourceFormat").unwrap_or("CSV");
            let autodetect = get_bool(edc, "autodetect");
            let connection_id = get_str(edc, "connectionId");
            let csv_skip_leading_rows = get_struct_fields(edc, "csvOptions")
                .and_then(|csv| get_number(csv, "skipLeadingRows"))
                .map(|n| n as i64);
            ExternalDataConfig {
                source_uris,
                source_format,
                autodetect,
                connection_id,
                csv_skip_leading_rows,
            }
        });

    Ok(TableInputs {
        project,
        dataset,
        table_id,
        description,
        friendly_name,
        labels,
        view,
        materialized_view,
        time_partitioning,
        range_partitioning,
        clusterings,
        deletion_protection,
        expiration_time,
        encryption_kms_key,
        storage_billing_model,
        max_staleness,
        external_data_config,
    })
}

pub fn build_table_output(
    inputs: &TableInputs<'_>,
    table_type: &str,
    num_rows: i64,
    creation_time: i64,
) -> prost_types::Struct {
    // Time partitioning — round-trip so diff sees matching olds/news.
    let tp_struct = inputs.time_partitioning.as_ref().map(|tp| {
        OutputBuilder::new()
            .str("type", tp.partition_type)
            .str_opt("field", tp.field)
            .num_opt("expirationMs", tp.expiration_ms)
            .build()
    });

    // Range partitioning.
    let rp_struct = inputs.range_partitioning.as_ref().map(|rp| {
        let range = OutputBuilder::new()
            .num("start", rp.start as f64)
            .num("end", rp.end as f64)
            .num("interval", rp.interval as f64)
            .build();
        OutputBuilder::new()
            .str("field", rp.field)
            .nested("range", range)
            .build()
    });

    // View.
    let view_struct = inputs.view.as_ref().map(|v| {
        OutputBuilder::new()
            .str("query", v.query)
            .bool_opt("useLegacySql", v.use_legacy_sql)
            .build()
    });

    // Materialized view.
    let mv_struct = inputs.materialized_view.as_ref().map(|mv| {
        OutputBuilder::new()
            .str("query", mv.query)
            .bool_opt("enableRefresh", mv.enable_refresh)
            .num_opt("refreshIntervalMs", mv.refresh_interval_ms)
            .str_opt("maxStaleness", mv.max_staleness)
            .build()
    });

    // External data configuration.
    let edc_struct = inputs.external_data_config.as_ref().map(|edc| {
        let csv_struct = edc.csv_skip_leading_rows.map(|skip| {
            OutputBuilder::new()
                .num("skipLeadingRows", skip as f64)
                .build()
        });
        OutputBuilder::new()
            .str_list("sourceUris", &edc.source_uris.to_vec())
            .str("sourceFormat", edc.source_format)
            .bool_opt("autodetect", edc.autodetect)
            .str_opt("connectionId", edc.connection_id)
            .nested_opt("csvOptions", csv_struct)
            .build()
    });

    let clusterings_ref: Vec<&str> = inputs.clusterings.to_vec();

    let mut builder = OutputBuilder::new()
        .str("project", inputs.project)
        .str("dataset", inputs.dataset)
        .str("tableId", inputs.table_id)
        .str("tableType", table_type)
        .num("numRows", num_rows as f64)
        .num("creationTime", creation_time as f64)
        .str_opt("description", inputs.description)
        .str_opt("friendlyName", inputs.friendly_name)
        .labels("labels", &inputs.labels)
        .bool_opt("deletionProtection", inputs.deletion_protection)
        .str_opt("storageBillingModel", inputs.storage_billing_model)
        .str_opt("maxStaleness", inputs.max_staleness)
        .num_opt("expirationTime", inputs.expiration_time)
        .str_opt("encryptionKmsKey", inputs.encryption_kms_key)
        .nested_opt("timePartitioning", tp_struct)
        .nested_opt("rangePartitioning", rp_struct)
        .nested_opt("view", view_struct)
        .nested_opt("materializedView", mv_struct)
        .nested_opt("externalDataConfiguration", edc_struct);

    if !inputs.clusterings.is_empty() {
        builder = builder.str_list("clusterings", &clusterings_ref);
    }

    builder.build()
}
