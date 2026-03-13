use std::collections::BTreeMap;

use crate::dbt::macros::MacroDef;
use crate::dbt::types::{
    MacroInputs, MacroOutput, ModelOutput, ModelRefData, ProjectContext, ProjectInputs, SourceDef,
    SourceInputs,
};
use crate::prost_util::{
    get_list, get_str, get_struct_fields, prost_list, prost_string, prost_struct, value_as_str,
};

pub fn parse_project_inputs(s: &prost_types::Struct) -> Result<ProjectInputs<'_>, &'static str> {
    let gcp_project = get_str(&s.fields, "gcpProject")
        .ok_or("missing required field 'gcpProject': provide a GCP project ID")?;
    let dataset = get_str(&s.fields, "dataset")
        .ok_or("missing required field 'dataset': provide a BigQuery dataset ID")?;

    let mut sources = Vec::new();
    if let Some(sf) = get_struct_fields(&s.fields, "sources") {
        for (name, v) in sf {
            if let Some(src_fields) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StructValue(s) => Some(&s.fields),
                _ => None,
            }) {
                let src_dataset = get_str(src_fields, "dataset").unwrap_or("");
                let tables = get_list(src_fields, "tables")
                    .map(|items| items.iter().filter_map(value_as_str).collect())
                    .unwrap_or_default();
                sources.push((
                    name.as_str(),
                    SourceInputs {
                        dataset: src_dataset,
                        tables,
                    },
                ));
            }
        }
    }

    let declared_models = get_list(&s.fields, "declaredModels")
        .map(|items| items.iter().filter_map(value_as_str).collect())
        .unwrap_or_default();

    let declared_macros = get_list(&s.fields, "declaredMacros")
        .map(|items| items.iter().filter_map(value_as_str).collect())
        .unwrap_or_default();

    let mut vars = Vec::new();
    if let Some(vf) = get_struct_fields(&s.fields, "vars") {
        for (name, v) in vf {
            if let Some(val) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StringValue(s) => Some(s.as_str()),
                _ => None,
            }) {
                vars.push((name.as_str(), val));
            }
        }
    }

    Ok(ProjectInputs {
        gcp_project,
        dataset,
        sources,
        declared_models,
        declared_macros,
        vars,
    })
}

pub fn build_project_context_output(inputs: &ProjectInputs<'_>) -> prost_types::Struct {
    let mut fields = BTreeMap::new();
    fields.insert("gcpProject".to_owned(), prost_string(inputs.gcp_project));
    fields.insert("dataset".to_owned(), prost_string(inputs.dataset));

    // Sources.
    let mut sources_fields = BTreeMap::new();
    for (name, src) in &inputs.sources {
        let mut sf = BTreeMap::new();
        sf.insert("dataset".to_owned(), prost_string(src.dataset));
        sf.insert(
            "tables".to_owned(),
            prost_list(src.tables.iter().map(|t| prost_string(t)).collect()),
        );
        sources_fields.insert(name.to_string(), prost_struct(sf));
    }
    fields.insert("sources".to_owned(), prost_struct(sources_fields));

    // Declared models.
    fields.insert(
        "declaredModels".to_owned(),
        prost_list(
            inputs
                .declared_models
                .iter()
                .map(|m| prost_string(m))
                .collect(),
        ),
    );
    // Declared macros.
    fields.insert(
        "declaredMacros".to_owned(),
        prost_list(
            inputs
                .declared_macros
                .iter()
                .map(|m| prost_string(m))
                .collect(),
        ),
    );

    // Vars.
    if !inputs.vars.is_empty() {
        let mut vars_fields = BTreeMap::new();
        for (name, val) in &inputs.vars {
            vars_fields.insert(name.to_string(), prost_string(val));
        }
        fields.insert("vars".to_owned(), prost_struct(vars_fields));
    }

    prost_types::Struct { fields }
}

pub fn parse_macro_inputs(s: &prost_types::Struct) -> Result<MacroInputs<'_>, &'static str> {
    let name = get_str(&s.fields, "name")
        .ok_or("missing required field 'name': provide a unique macro identifier")?;
    let sql = get_str(&s.fields, "sql")
        .ok_or("missing required field 'sql': provide the macro SQL body")?;
    let args = get_list(&s.fields, "args")
        .map(|items| items.iter().filter_map(value_as_str).collect())
        .unwrap_or_default();

    Ok(MacroInputs { name, args, sql })
}

pub fn build_macro_output(output: &MacroOutput) -> prost_types::Struct {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_owned(), prost_string(&output.name));
    fields.insert("sql".to_owned(), prost_string(&output.sql));
    fields.insert(
        "args".to_owned(),
        prost_list(output.args.iter().map(|a| prost_string(a)).collect()),
    );
    prost_types::Struct { fields }
}

pub fn build_model_output_struct(output: &ModelOutput) -> prost_types::Struct {
    let mut fields = BTreeMap::new();
    fields.insert(
        "materialization".to_owned(),
        prost_string(&output.materialization),
    );
    fields.insert(
        "resolvedCtesJson".to_owned(),
        prost_string(&output.resolved_ctes_json),
    );
    fields.insert(
        "resolvedBody".to_owned(),
        prost_string(&output.resolved_body),
    );
    fields.insert("tableRef".to_owned(), prost_string(&output.table_ref));
    fields.insert("resolvedDdl".to_owned(), prost_string(&output.resolved_ddl));
    fields.insert("resolvedSql".to_owned(), prost_string(&output.resolved_sql));
    fields.insert(
        "workflowYaml".to_owned(),
        prost_string(&output.workflow_yaml),
    );
    fields.insert("tableId".to_owned(), prost_string(&output.table_id));
    fields.insert(
        "schedulerSql".to_owned(),
        prost_string(&output.scheduler_sql),
    );
    if let Some(bytes) = output.estimated_bytes_processed {
        fields.insert(
            "estimatedBytesProcessed".to_owned(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(bytes as f64)),
            },
        );
    }
    if let Some(ref cost) = output.estimated_cost {
        fields.insert("estimatedCost".to_owned(), prost_string(cost));
    }
    if let Some(limit) = output.max_bytes_billed {
        fields.insert(
            "maxBytesBilled".to_owned(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(limit as f64)),
            },
        );
    }
    prost_types::Struct { fields }
}

pub fn parse_project_context(fields: &BTreeMap<String, prost_types::Value>) -> ProjectContext<'_> {
    let gcp_project = get_str(fields, "gcpProject").unwrap_or("");
    let dataset = get_str(fields, "dataset").unwrap_or("");

    let mut sources = BTreeMap::new();
    if let Some(sf) = get_struct_fields(fields, "sources") {
        for (name, v) in sf {
            if let Some(src_fields) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StructValue(s) => Some(&s.fields),
                _ => None,
            }) {
                let src_dataset = get_str(src_fields, "dataset").unwrap_or("").to_owned();
                let tables = get_list(src_fields, "tables")
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(value_as_str)
                            .map(|s| s.to_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                sources.insert(
                    name.clone(),
                    SourceDef {
                        dataset: src_dataset,
                        tables,
                    },
                );
            }
        }
    }

    let declared_models = get_list(fields, "declaredModels")
        .map(|items| {
            items
                .iter()
                .filter_map(value_as_str)
                .map(|s| s.to_owned())
                .collect()
        })
        .unwrap_or_default();

    let declared_macros = get_list(fields, "declaredMacros")
        .map(|items| {
            items
                .iter()
                .filter_map(value_as_str)
                .map(|s| s.to_owned())
                .collect()
        })
        .unwrap_or_default();

    let mut vars = BTreeMap::new();
    if let Some(vf) = get_struct_fields(fields, "vars") {
        for (name, v) in vf {
            if let Some(val) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StringValue(s) => Some(s.as_str()),
                _ => None,
            }) {
                vars.insert(name.clone(), val.to_owned());
            }
        }
    }

    ProjectContext {
        gcp_project,
        dataset,
        sources,
        declared_models,
        declared_macros,
        vars,
    }
}

pub fn parse_model_refs(
    fields: &BTreeMap<String, prost_types::Value>,
) -> BTreeMap<String, ModelRefData> {
    let mut refs = BTreeMap::new();
    if let Some(rf) = get_struct_fields(fields, "modelRefs") {
        for (name, v) in rf {
            if let Some(ref_fields) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StructValue(s) => Some(&s.fields),
                _ => None,
            }) {
                refs.insert(
                    name.clone(),
                    ModelRefData {
                        materialization: get_str(ref_fields, "materialization")
                            .unwrap_or("")
                            .to_owned(),
                        resolved_ctes_json: get_str(ref_fields, "resolvedCtesJson")
                            .unwrap_or("")
                            .to_owned(),
                        resolved_body: get_str(ref_fields, "resolvedBody").unwrap_or("").to_owned(),
                        table_ref: get_str(ref_fields, "tableRef").unwrap_or("").to_owned(),
                        resolved_ddl: get_str(ref_fields, "resolvedDdl").unwrap_or("").to_owned(),
                        resolved_sql: get_str(ref_fields, "resolvedSql").unwrap_or("").to_owned(),
                        workflow_yaml: get_str(ref_fields, "workflowYaml").unwrap_or("").to_owned(),
                    },
                );
            }
        }
    }
    refs
}

pub fn parse_macro_defs(
    fields: &BTreeMap<String, prost_types::Value>,
) -> BTreeMap<String, MacroDef> {
    let mut defs = BTreeMap::new();
    if let Some(mf) = get_struct_fields(fields, "macros") {
        for (name, v) in mf {
            if let Some(macro_fields) = v.kind.as_ref().and_then(|k| match k {
                prost_types::value::Kind::StructValue(s) => Some(&s.fields),
                _ => None,
            }) {
                let args = get_list(macro_fields, "args")
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(value_as_str)
                            .map(|s| s.to_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                let sql = get_str(macro_fields, "sql").unwrap_or("").to_owned();
                defs.insert(name.clone(), MacroDef { args, sql });
            }
        }
    }
    defs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost_util::{prost_list, prost_string, prost_struct};

    fn make_prost_struct(fields: Vec<(&str, prost_types::Value)>) -> prost_types::Struct {
        prost_types::Struct {
            fields: fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        }
    }

    fn make_source_struct(dataset: &str, tables: Vec<&str>) -> prost_types::Value {
        let mut sf = BTreeMap::new();
        sf.insert("dataset".to_owned(), prost_string(dataset));
        sf.insert(
            "tables".to_owned(),
            prost_list(tables.iter().map(|t| prost_string(t)).collect()),
        );
        prost_struct(sf)
    }

    // --- parse_project_inputs ---

    #[test]
    fn parse_project_inputs_valid() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "raw".to_owned(),
            make_source_struct("raw_data", vec!["customers"]),
        );

        let s = make_prost_struct(vec![
            ("gcpProject", prost_string("my-proj")),
            ("dataset", prost_string("analytics")),
            ("sources", prost_struct(sources)),
            ("declaredModels", prost_list(vec![prost_string("stg")])),
            ("declaredMacros", prost_list(vec![prost_string("sk")])),
        ]);

        let inputs = parse_project_inputs(&s).unwrap();
        assert_eq!(inputs.gcp_project, "my-proj");
        assert_eq!(inputs.dataset, "analytics");
        assert_eq!(inputs.sources.len(), 1);
        assert_eq!(inputs.sources[0].0, "raw");
        assert_eq!(inputs.sources[0].1.dataset, "raw_data");
        assert_eq!(inputs.sources[0].1.tables, vec!["customers"]);
        assert_eq!(inputs.declared_models, vec!["stg"]);
        assert_eq!(inputs.declared_macros, vec!["sk"]);
    }

    #[test]
    fn parse_project_inputs_missing_gcp_project() {
        let s = make_prost_struct(vec![("dataset", prost_string("ds"))]);
        let err = parse_project_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'gcpProject'"));
    }

    #[test]
    fn parse_project_inputs_missing_dataset() {
        let s = make_prost_struct(vec![("gcpProject", prost_string("proj"))]);
        let err = parse_project_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'dataset'"));
    }

    #[test]
    fn parse_project_inputs_with_sources() {
        let mut sources = BTreeMap::new();
        sources.insert(
            "staging".to_owned(),
            make_source_struct("stg_data", vec!["orders", "users"]),
        );
        sources.insert(
            "prod".to_owned(),
            make_source_struct("prod_data", vec!["items"]),
        );

        let s = make_prost_struct(vec![
            ("gcpProject", prost_string("proj")),
            ("dataset", prost_string("ds")),
            ("sources", prost_struct(sources)),
        ]);

        let inputs = parse_project_inputs(&s).unwrap();
        assert_eq!(inputs.sources.len(), 2);
    }

    #[test]
    fn parse_project_inputs_empty_sources() {
        let s = make_prost_struct(vec![
            ("gcpProject", prost_string("proj")),
            ("dataset", prost_string("ds")),
        ]);
        let inputs = parse_project_inputs(&s).unwrap();
        assert!(inputs.sources.is_empty());
        assert!(inputs.declared_models.is_empty());
        assert!(inputs.declared_macros.is_empty());
    }

    // --- build_project_context → parse_project_context roundtrip ---

    #[test]
    fn build_project_context_roundtrip() {
        let inputs = ProjectInputs {
            gcp_project: "proj",
            dataset: "ds",
            sources: vec![(
                "raw",
                SourceInputs {
                    dataset: "raw_data",
                    tables: vec!["customers", "orders"],
                },
            )],
            declared_models: vec!["stg", "mart"],
            declared_macros: vec!["sk"],
            vars: vec![("days", "90")],
        };

        let output = build_project_context_output(&inputs);
        let ctx = parse_project_context(&output.fields);

        assert_eq!(ctx.gcp_project, "proj");
        assert_eq!(ctx.dataset, "ds");
        assert_eq!(ctx.sources.len(), 1);
        let raw = ctx.sources.get("raw").unwrap();
        assert_eq!(raw.dataset, "raw_data");
        assert_eq!(raw.tables, vec!["customers", "orders"]);
        assert_eq!(ctx.declared_models, vec!["stg", "mart"]);
        assert_eq!(ctx.declared_macros, vec!["sk"]);
        assert_eq!(ctx.vars.get("days").map(|s| s.as_str()), Some("90"));
    }

    // --- parse_macro_inputs ---

    #[test]
    fn parse_macro_inputs_valid() {
        let s = make_prost_struct(vec![
            ("name", prost_string("my_macro")),
            ("sql", prost_string("SELECT {{ x }}")),
            ("args", prost_list(vec![prost_string("x")])),
        ]);

        let inputs = parse_macro_inputs(&s).unwrap();
        assert_eq!(inputs.name, "my_macro");
        assert_eq!(inputs.sql, "SELECT {{ x }}");
        assert_eq!(inputs.args, vec!["x"]);
    }

    #[test]
    fn parse_macro_inputs_missing_name() {
        let s = make_prost_struct(vec![("sql", prost_string("SELECT 1"))]);
        let err = parse_macro_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'name'"));
    }

    #[test]
    fn parse_macro_inputs_missing_sql() {
        let s = make_prost_struct(vec![("name", prost_string("m"))]);
        let err = parse_macro_inputs(&s).unwrap_err();
        assert!(err.contains("missing required field 'sql'"));
    }

    #[test]
    fn parse_macro_inputs_no_args() {
        let s = make_prost_struct(vec![
            ("name", prost_string("now")),
            ("sql", prost_string("CURRENT_TIMESTAMP()")),
        ]);
        let inputs = parse_macro_inputs(&s).unwrap();
        assert!(inputs.args.is_empty());
    }

    // --- build_macro_output ---

    #[test]
    fn build_macro_output_roundtrip() {
        let output = MacroOutput {
            name: "sk".to_owned(),
            args: vec!["cols".to_owned()],
            sql: "MD5({{ cols }})".to_owned(),
        };
        let s = build_macro_output(&output);
        assert_eq!(get_str(&s.fields, "name"), Some("sk"));
        assert_eq!(get_str(&s.fields, "sql"), Some("MD5({{ cols }})"));
        let args = get_list(&s.fields, "args").unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(value_as_str(&args[0]), Some("cols"));
    }

    // --- build_model_output_struct ---

    #[test]
    fn build_model_output_struct_all_fields() {
        let output = ModelOutput {
            materialization: "table".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: "SELECT 1".to_owned(),
            table_ref: "`p.d.t`".to_owned(),
            resolved_ddl: "CREATE TABLE".to_owned(),
            resolved_sql: "SELECT 1".to_owned(),
            workflow_yaml: "- runQuery:".to_owned(),
            table_id: "t".to_owned(),
            scheduler_sql: "MERGE ...".to_owned(),
            estimated_bytes_processed: None,
            estimated_cost: None,
            max_bytes_billed: None,
        };
        let s = build_model_output_struct(&output);
        assert_eq!(s.fields.len(), 9);
        assert_eq!(get_str(&s.fields, "materialization"), Some("table"));
        assert_eq!(get_str(&s.fields, "resolvedCtesJson"), Some("[]"));
        assert_eq!(get_str(&s.fields, "resolvedBody"), Some("SELECT 1"));
        assert_eq!(get_str(&s.fields, "tableRef"), Some("`p.d.t`"));
        assert_eq!(get_str(&s.fields, "resolvedDdl"), Some("CREATE TABLE"));
        assert_eq!(get_str(&s.fields, "resolvedSql"), Some("SELECT 1"));
        assert_eq!(get_str(&s.fields, "workflowYaml"), Some("- runQuery:"));
        assert_eq!(get_str(&s.fields, "tableId"), Some("t"));
        assert_eq!(get_str(&s.fields, "schedulerSql"), Some("MERGE ..."));
    }

    // --- parse_model_refs ---

    #[test]
    fn parse_model_refs_valid() {
        let mut ref_fields = BTreeMap::new();
        ref_fields.insert("materialization".to_owned(), prost_string("view"));
        ref_fields.insert("resolvedCtesJson".to_owned(), prost_string("[]"));
        ref_fields.insert("resolvedBody".to_owned(), prost_string("SELECT *"));
        ref_fields.insert("tableRef".to_owned(), prost_string("`p.d.stg`"));
        ref_fields.insert("resolvedDdl".to_owned(), prost_string("CREATE VIEW"));
        ref_fields.insert("resolvedSql".to_owned(), prost_string("SELECT *"));
        ref_fields.insert("workflowYaml".to_owned(), prost_string("yaml"));

        let mut model_refs = BTreeMap::new();
        model_refs.insert("stg".to_owned(), prost_struct(ref_fields));

        let mut fields = BTreeMap::new();
        fields.insert("modelRefs".to_owned(), prost_struct(model_refs));

        let refs = parse_model_refs(&fields);
        assert_eq!(refs.len(), 1);
        let stg = refs.get("stg").unwrap();
        assert_eq!(stg.materialization, "view");
        assert_eq!(stg.table_ref, "`p.d.stg`");
        assert_eq!(stg.resolved_body, "SELECT *");
        assert_eq!(stg.resolved_ddl, "CREATE VIEW");
        assert_eq!(stg.resolved_sql, "SELECT *");
        assert_eq!(stg.workflow_yaml, "yaml");
    }

    #[test]
    fn parse_model_refs_empty() {
        let fields = BTreeMap::new();
        let refs = parse_model_refs(&fields);
        assert!(refs.is_empty());
    }

    // --- parse_macro_defs ---

    #[test]
    fn parse_macro_defs_valid() {
        let mut macro_fields = BTreeMap::new();
        macro_fields.insert("args".to_owned(), prost_list(vec![prost_string("x")]));
        macro_fields.insert("sql".to_owned(), prost_string("LOWER({{ x }})"));

        let mut macros = BTreeMap::new();
        macros.insert("lower_col".to_owned(), prost_struct(macro_fields));

        let mut fields = BTreeMap::new();
        fields.insert("macros".to_owned(), prost_struct(macros));

        let defs = parse_macro_defs(&fields);
        assert_eq!(defs.len(), 1);
        let lower = defs.get("lower_col").unwrap();
        assert_eq!(lower.args, vec!["x"]);
        assert_eq!(lower.sql, "LOWER({{ x }})");
    }

    #[test]
    fn parse_macro_defs_empty() {
        let fields = BTreeMap::new();
        let defs = parse_macro_defs(&fields);
        assert!(defs.is_empty());
    }
}
