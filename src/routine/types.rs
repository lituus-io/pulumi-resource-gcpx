pub struct RoutineArgument<'a> {
    pub name: &'a str,
    pub data_type: &'a str,
    pub mode: Option<&'a str>,
}

pub struct RoutineInputs<'a> {
    pub project: &'a str,
    pub dataset: &'a str,
    pub routine_id: &'a str,
    pub routine_type: &'a str, // SCALAR_FUNCTION | TABLE_VALUED_FUNCTION | PROCEDURE
    pub language: &'a str,     // SQL | JAVASCRIPT
    pub definition_body: &'a str,
    pub description: Option<&'a str>,
    pub arguments: Vec<RoutineArgument<'a>>,
    pub return_type: Option<&'a str>,
    pub imported_libraries: Vec<&'a str>,
    pub determinism_level: Option<&'a str>,
}
