use crate::json_body::JsonBody;
use crate::routine::types::RoutineInputs;

pub fn build_create_body(inputs: &RoutineInputs<'_>) -> serde_json::Value {
    let mut body = JsonBody::new()
        .object(
            "routineReference",
            serde_json::json!({
                "projectId": inputs.project,
                "datasetId": inputs.dataset,
                "routineId": inputs.routine_id,
            }),
        )
        .str("routineType", inputs.routine_type)
        .str("language", inputs.language)
        .str("definitionBody", inputs.definition_body)
        .str_opt("description", inputs.description)
        .str_opt("determinismLevel", inputs.determinism_level);

    if let Some(rt) = inputs.return_type {
        body = body.object("returnType", serde_json::json!({ "typeKind": rt }));
    }

    if !inputs.arguments.is_empty() {
        let args: Vec<serde_json::Value> = inputs
            .arguments
            .iter()
            .map(|a| {
                let mut arg = serde_json::json!({
                    "name": a.name,
                    "dataType": { "typeKind": a.data_type },
                });
                if let Some(mode) = a.mode {
                    arg.as_object_mut()
                        .unwrap()
                        .insert("mode".into(), serde_json::Value::String(mode.into()));
                }
                arg
            })
            .collect();
        body = body.object("arguments", serde_json::Value::Array(args));
    }

    if !inputs.imported_libraries.is_empty() {
        let libs: Vec<serde_json::Value> = inputs
            .imported_libraries
            .iter()
            .map(|l| serde_json::Value::String(l.to_string()))
            .collect();
        body = body.object("importedLibraries", serde_json::Value::Array(libs));
    }

    body.build()
}

/// BQ Routines API uses PUT (full replace) for updates, so we build the complete body.
pub fn build_update_body(inputs: &RoutineInputs<'_>) -> serde_json::Value {
    build_create_body(inputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routine::types::RoutineInputs;

    fn base<'a>() -> RoutineInputs<'a> {
        RoutineInputs {
            project: "p",
            dataset: "d",
            routine_id: "r",
            routine_type: "SCALAR_FUNCTION",
            language: "SQL",
            definition_body: "x * 100",
            description: None,
            arguments: vec![],
            return_type: None,
            imported_libraries: vec![],
            determinism_level: None,
        }
    }

    #[test]
    fn create_body_basic() {
        let body = build_create_body(&base());
        assert_eq!(body["routineReference"]["routineId"], "r");
        assert_eq!(body["routineType"], "SCALAR_FUNCTION");
        assert_eq!(body["language"], "SQL");
        assert_eq!(body["definitionBody"], "x * 100");
    }

    #[test]
    fn create_body_with_description() {
        let mut inputs = base();
        inputs.description = Some("my func");
        let body = build_create_body(&inputs);
        assert_eq!(body["description"], "my func");
    }

    #[test]
    fn update_body_same_as_create() {
        let inputs = base();
        let create = build_create_body(&inputs);
        let update = build_update_body(&inputs);
        assert_eq!(create, update);
    }
}
