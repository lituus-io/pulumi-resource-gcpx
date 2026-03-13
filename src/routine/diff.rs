use crate::resource::DiffResult;
use crate::routine::types::RoutineInputs;

pub fn compute_routine_diff(old: &RoutineInputs<'_>, new: &RoutineInputs<'_>) -> DiffResult {
    let mut replace_keys = Vec::new();
    let mut update_keys = Vec::new();

    // Replace triggers.
    if old.project != new.project {
        replace_keys.push("project");
    }
    if old.dataset != new.dataset {
        replace_keys.push("dataset");
    }
    if old.routine_id != new.routine_id {
        replace_keys.push("routineId");
    }
    if old.routine_type != new.routine_type {
        replace_keys.push("routineType");
    }
    if old.language != new.language {
        replace_keys.push("language");
    }

    // Update triggers.
    if old.definition_body != new.definition_body {
        update_keys.push("definitionBody");
    }
    if old.description != new.description {
        update_keys.push("description");
    }
    if old.return_type != new.return_type {
        update_keys.push("returnType");
    }
    if old.determinism_level != new.determinism_level {
        update_keys.push("determinismLevel");
    }
    if old.imported_libraries != new.imported_libraries {
        update_keys.push("importedLibraries");
    }

    // Compare arguments by checking if they differ.
    let args_differ = old.arguments.len() != new.arguments.len()
        || old
            .arguments
            .iter()
            .zip(new.arguments.iter())
            .any(|(a, b)| a.name != b.name || a.data_type != b.data_type || a.mode != b.mode);
    if args_differ {
        update_keys.push("arguments");
    }

    DiffResult {
        replace_keys,
        update_keys,
    }
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
    fn no_changes() {
        let diff = compute_routine_diff(&base(), &base());
        assert!(!diff.has_changes());
    }

    #[test]
    fn language_replaces() {
        let old = base();
        let mut new = base();
        new.language = "JAVASCRIPT";
        let diff = compute_routine_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"language"));
    }

    #[test]
    fn definition_body_updates() {
        let old = base();
        let mut new = base();
        new.definition_body = "x * 200";
        let diff = compute_routine_diff(&old, &new);
        assert!(diff.has_changes());
        assert!(!diff.needs_replace());
        assert!(diff.update_keys.contains(&"definitionBody"));
    }

    #[test]
    fn project_replaces() {
        let old = base();
        let mut new = base();
        new.project = "other";
        let diff = compute_routine_diff(&old, &new);
        assert!(diff.needs_replace());
        assert!(diff.replace_keys.contains(&"project"));
    }
}
