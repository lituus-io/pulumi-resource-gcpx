/// Generate GCP Workflow YAML source contents from a SQL query.
///
/// Output format matches `gcp:workflows:Workflow.sourceContents` and
/// `googleapis.bigquery.v2.jobs.query` connector syntax.
pub fn generate_workflow_yaml(project: &str, sql: &str) -> String {
    let escaped_sql = escape_sql_for_yaml(sql);
    format!(
        "- runQuery:\n    call: googleapis.bigquery.v2.jobs.query\n    args:\n        projectId: {project}\n        body:\n            useLegacySql: false\n            useQueryCache: false\n            timeoutMs: 600000\n            query: |\n                {escaped_sql}"
    )
}

/// Escape SQL for safe embedding in YAML block scalar.
/// Handles indentation alignment for multi-line SQL.
fn escape_sql_for_yaml(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len() + 256);
    for (i, line) in sql.lines().enumerate() {
        if i > 0 {
            result.push('\n');
            // 16-space indent for YAML block continuation.
            result.push_str("                ");
        }
        result.push_str(line);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_line_sql() {
        let yaml = generate_workflow_yaml("my-project", "SELECT 1");
        assert!(yaml.contains("googleapis.bigquery.v2.jobs.query"));
        assert!(yaml.contains("my-project"));
        assert!(yaml.contains("SELECT 1"));
    }

    #[test]
    fn multi_line_sql() {
        let sql = "CREATE OR REPLACE TABLE `p.d.t` AS\nSELECT *\nFROM source";
        let yaml = generate_workflow_yaml("proj", sql);
        assert!(yaml.contains("CREATE OR REPLACE TABLE"));
        assert!(yaml.contains("FROM source"));
        // Each continuation line should be indented.
        let lines: Vec<&str> = yaml.lines().collect();
        let query_line = lines
            .iter()
            .position(|l| l.contains("CREATE OR REPLACE"))
            .unwrap();
        assert!(lines[query_line + 1].starts_with("                "));
    }

    #[test]
    fn empty_sql() {
        let yaml = generate_workflow_yaml("p", "");
        assert!(yaml.contains("query: |"));
    }

    #[test]
    fn special_characters_in_sql() {
        let sql = "SELECT * FROM `p.d.t` WHERE name = 'O\\'Brien'";
        let yaml = generate_workflow_yaml("p", sql);
        assert!(yaml.contains("O\\'Brien"));
    }

    #[test]
    fn yaml_structure() {
        let yaml = generate_workflow_yaml("p", "SELECT 1");
        assert!(yaml.starts_with("- runQuery:"));
        assert!(yaml.contains("useLegacySql: false"));
        assert!(yaml.contains("useQueryCache: false"));
        assert!(yaml.contains("timeoutMs: 600000"));
    }
}
