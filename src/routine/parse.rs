use crate::output::OutputBuilder;
use crate::prost_util::{get_list, get_str, value_as_str};
use crate::routine::types::{RoutineArgument, RoutineInputs};

pub fn parse_routine_inputs(s: &prost_types::Struct) -> Result<RoutineInputs<'_>, &'static str> {
    let project = get_str(&s.fields, "project")
        .ok_or("missing required field 'project': provide a GCP project ID")?;
    let dataset = get_str(&s.fields, "dataset")
        .ok_or("missing required field 'dataset': provide a BigQuery dataset ID")?;
    let routine_id = get_str(&s.fields, "routineId")
        .ok_or("missing required field 'routineId': provide a unique routine name")?;
    let routine_type = get_str(&s.fields, "routineType").ok_or("missing required field 'routineType': must be SCALAR_FUNCTION, TABLE_VALUED_FUNCTION, or PROCEDURE")?;
    let language = get_str(&s.fields, "language")
        .ok_or("missing required field 'language': must be SQL or JAVASCRIPT")?;
    let definition_body = get_str(&s.fields, "definitionBody").ok_or(
        "missing required field 'definitionBody': provide the routine implementation code",
    )?;
    let description = get_str(&s.fields, "description");
    let return_type = get_str(&s.fields, "returnType");
    let determinism_level = get_str(&s.fields, "determinismLevel");

    let mut arguments = Vec::new();
    if let Some(args) = get_list(&s.fields, "arguments") {
        for arg_val in args {
            if let Some(prost_types::value::Kind::StructValue(arg_struct)) = &arg_val.kind {
                let name = get_str(&arg_struct.fields, "name").unwrap_or("");
                let data_type = get_str(&arg_struct.fields, "dataType").unwrap_or("");
                let mode = get_str(&arg_struct.fields, "mode");
                arguments.push(RoutineArgument {
                    name,
                    data_type,
                    mode,
                });
            }
        }
    }

    let imported_libraries = get_list(&s.fields, "importedLibraries")
        .map(|items| items.iter().filter_map(value_as_str).collect::<Vec<_>>())
        .unwrap_or_default();

    Ok(RoutineInputs {
        project,
        dataset,
        routine_id,
        routine_type,
        language,
        definition_body,
        description,
        arguments,
        return_type,
        imported_libraries,
        determinism_level,
    })
}

pub fn build_routine_output(
    inputs: &RoutineInputs<'_>,
    meta: &crate::bq::RoutineMeta,
) -> prost_types::Struct {
    let mut builder = OutputBuilder::new()
        .str("project", inputs.project)
        .str("dataset", inputs.dataset)
        .str("routineId", inputs.routine_id)
        .str("routineType", inputs.routine_type)
        .str("language", inputs.language)
        .str("definitionBody", inputs.definition_body)
        .num("creationTime", meta.creation_time as f64)
        .num("lastModifiedTime", meta.last_modified_time as f64)
        .str("etag", &meta.etag)
        .str_opt("description", inputs.description)
        .str_opt("returnType", inputs.return_type)
        .str_opt("determinismLevel", inputs.determinism_level);

    if !inputs.arguments.is_empty() {
        let args: Vec<prost_types::Value> = inputs
            .arguments
            .iter()
            .map(|a| {
                OutputBuilder::new()
                    .str("name", a.name)
                    .str("dataType", a.data_type)
                    .str_opt("mode", a.mode)
                    .build_value()
            })
            .collect();
        builder = builder.list("arguments", args);
    }
    if !inputs.imported_libraries.is_empty() {
        let libs: Vec<&str> = inputs.imported_libraries.to_vec();
        builder = builder.str_list("importedLibraries", &libs);
    }

    builder.build()
}
