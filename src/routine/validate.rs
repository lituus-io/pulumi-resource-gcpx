use crate::routine::types::RoutineInputs;
use crate::schema::types::CheckFailure;

pub fn validate_routine(inputs: &RoutineInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    if inputs.project.is_empty() {
        failures.push(CheckFailure {
            property: "project".into(),
            reason: "project must not be empty".into(),
        });
    }
    if inputs.dataset.is_empty() {
        failures.push(CheckFailure {
            property: "dataset".into(),
            reason: "dataset must not be empty".into(),
        });
    }
    if inputs.routine_id.is_empty() {
        failures.push(CheckFailure {
            property: "routineId".into(),
            reason: "routineId must not be empty".into(),
        });
    }
    if !matches!(
        inputs.routine_type,
        "SCALAR_FUNCTION" | "TABLE_VALUED_FUNCTION" | "PROCEDURE"
    ) {
        failures.push(CheckFailure {
            property: "routineType".into(),
            reason: "routineType must be SCALAR_FUNCTION, TABLE_VALUED_FUNCTION, or PROCEDURE (case-sensitive)".into(),
        });
    }
    if !matches!(inputs.language, "SQL" | "JAVASCRIPT") {
        failures.push(CheckFailure {
            property: "language".into(),
            reason: "language must be 'SQL' or 'JAVASCRIPT' (case-sensitive)".into(),
        });
    }
    if inputs.definition_body.is_empty() {
        failures.push(CheckFailure {
            property: "definitionBody".into(),
            reason: "definitionBody must not be empty".into(),
        });
    }

    if let Some(dl) = inputs.determinism_level {
        if !matches!(
            dl,
            "DETERMINISM_LEVEL_UNSPECIFIED" | "DETERMINISTIC" | "NOT_DETERMINISTIC"
        ) {
            failures.push(CheckFailure {
                property: "determinismLevel".into(),
                reason: "determinismLevel must be DETERMINISM_LEVEL_UNSPECIFIED, DETERMINISTIC, or NOT_DETERMINISTIC (case-sensitive)".into(),
            });
        }
    }

    // JS UDFs require importedLibraries or definition only; SQL UDFs should not have importedLibraries.
    if inputs.language == "SQL" && !inputs.imported_libraries.is_empty() {
        failures.push(CheckFailure {
            property: "importedLibraries".into(),
            reason: "importedLibraries is only valid for JAVASCRIPT routines; remove it or set language to JAVASCRIPT".into(),
        });
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routine::types::RoutineInputs;

    fn base<'a>() -> RoutineInputs<'a> {
        RoutineInputs {
            project: "proj",
            dataset: "ds",
            routine_id: "my_func",
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
    fn valid_routine() {
        assert!(validate_routine(&base()).is_empty());
    }

    #[test]
    fn empty_project() {
        let mut inputs = base();
        inputs.project = "";
        let failures = validate_routine(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("project must not be empty")));
    }

    #[test]
    fn invalid_type() {
        let mut inputs = base();
        inputs.routine_type = "INVALID";
        let failures = validate_routine(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("routineType")));
    }

    #[test]
    fn invalid_language() {
        let mut inputs = base();
        inputs.language = "PYTHON";
        let failures = validate_routine(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("language")));
    }

    #[test]
    fn empty_definition_body() {
        let mut inputs = base();
        inputs.definition_body = "";
        let failures = validate_routine(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("definitionBody")));
    }

    #[test]
    fn sql_with_imported_libraries() {
        let mut inputs = base();
        inputs.imported_libraries = vec!["gs://bucket/lib.js"];
        let failures = validate_routine(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("importedLibraries")));
    }
}
