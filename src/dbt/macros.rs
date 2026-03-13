use std::collections::BTreeMap;

use crate::dbt::scanner::{DbtScanner, DbtSegment};

/// A macro definition: named SQL template with positional arguments.
#[derive(Clone)]
pub struct MacroDef {
    pub args: Vec<String>,
    pub sql: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MacroError {
    #[error("unknown macro '{0}'")]
    Unknown(String),
    #[error("macro '{name}' expects {expected} args, got {got}")]
    ArgCount {
        name: String,
        expected: usize,
        got: usize,
    },
    #[error("macro expansion did not converge after {0} iterations (possible recursion)")]
    RecursionLimit(usize),
    #[error("input too large ({0} bytes, max {1})")]
    InputTooLarge(usize, usize),
}

/// Expand all macro calls in SQL.
///
/// Iteratively expands until no Call segments remain (supports nested macros).
/// Returns owned string (allocation necessary).
/// Max 10 iterations to prevent infinite loops.
/// Maximum input size for macro expansion (1 MiB).
const MAX_INPUT_SIZE: usize = 1 << 20;

pub fn expand_macros(sql: &str, macros: &BTreeMap<String, MacroDef>) -> Result<String, MacroError> {
    if sql.len() > MAX_INPUT_SIZE {
        return Err(MacroError::InputTooLarge(sql.len(), MAX_INPUT_SIZE));
    }
    if macros.is_empty() {
        return Ok(sql.to_owned());
    }

    let mut current = sql.to_owned();
    let max_iterations = 10;

    for iteration in 0..max_iterations {
        let mut result = String::with_capacity(current.len());
        let mut had_expansion = false;

        for segment in DbtScanner::new(&current) {
            match segment {
                DbtSegment::Call { name, raw_args } => {
                    if let Some(macro_def) = macros.get(name) {
                        let args = parse_call_args(raw_args);
                        if args.len() != macro_def.args.len() {
                            return Err(MacroError::ArgCount {
                                name: name.to_owned(),
                                expected: macro_def.args.len(),
                                got: args.len(),
                            });
                        }

                        // Substitute arguments into macro SQL.
                        let expanded = substitute_args(&macro_def.sql, &macro_def.args, &args);
                        result.push_str(&expanded);
                        had_expansion = true;
                    } else {
                        // Unknown macro — leave as-is (will be caught by validation).
                        result.push_str("{{ ");
                        result.push_str(name);
                        result.push('(');
                        result.push_str(raw_args);
                        result.push_str(") }}");
                    }
                }
                DbtSegment::Sql(s) => result.push_str(s),
                DbtSegment::Config { raw_args } => {
                    result.push_str("{{ config(");
                    result.push_str(raw_args);
                    result.push_str(") }}");
                }
                DbtSegment::Ref { model } => {
                    result.push_str("{{ ref('");
                    result.push_str(model);
                    result.push_str("') }}");
                }
                DbtSegment::Source { source, table } => {
                    result.push_str("{{ source('");
                    result.push_str(source);
                    result.push_str("', '");
                    result.push_str(table);
                    result.push_str("') }}");
                }
            }
        }

        if !had_expansion {
            return Ok(result);
        }

        if result.len() > MAX_INPUT_SIZE {
            return Err(MacroError::InputTooLarge(result.len(), MAX_INPUT_SIZE));
        }
        current = result;

        if iteration == max_iterations - 1 {
            return Err(MacroError::RecursionLimit(max_iterations));
        }
    }

    Ok(current)
}

/// Parse positional arguments from a raw_args string.
/// Supports single-quoted strings and unquoted values.
fn parse_call_args(raw_args: &str) -> Vec<&str> {
    if raw_args.trim().is_empty() {
        return Vec::new();
    }

    let mut args = Vec::new();
    let trimmed = raw_args.trim();

    // Try to extract single-quoted arguments.
    if trimmed.starts_with('\'') {
        let mut remaining = trimmed;
        while !remaining.is_empty() {
            remaining = remaining.trim();
            if remaining.starts_with('\'') {
                if let Some(end) = remaining[1..].find('\'') {
                    args.push(&remaining[1..1 + end]);
                    remaining = &remaining[2 + end..];
                    remaining = remaining.trim_start_matches(',').trim();
                } else {
                    args.push(remaining);
                    break;
                }
            } else {
                // Unquoted argument.
                match remaining.find(',') {
                    Some(comma) => {
                        args.push(remaining[..comma].trim());
                        remaining = &remaining[comma + 1..];
                    }
                    None => {
                        args.push(remaining.trim());
                        break;
                    }
                }
            }
        }
    } else {
        // All unquoted — single argument (raw text).
        args.push(trimmed);
    }

    args
}

/// Substitute `{{ arg_name }}` placeholders in macro SQL with actual values.
fn substitute_args(sql: &str, param_names: &[String], arg_values: &[&str]) -> String {
    let mut result = sql.to_owned();
    for (name, value) in param_names.iter().zip(arg_values.iter()) {
        // Replace {{ name }} with value.
        let placeholder_spaced = format!("{{{{ {} }}}}", name);
        result = result.replace(&placeholder_spaced, value);
        // Also try without spaces: {{name}}.
        let placeholder_tight = format!("{{{{{}}}}}", name);
        result = result.replace(&placeholder_tight, value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_single_macro() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "sk".to_owned(),
            MacroDef {
                args: vec!["cols".to_owned()],
                sql: "TO_HEX(MD5(CONCAT({{ cols }})))".to_owned(),
            },
        );

        let sql = "SELECT {{ sk('id, email') }} FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT TO_HEX(MD5(CONCAT(id, email))) FROM t");
    }

    #[test]
    fn expand_nested_macros() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "inner".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "LOWER({{ x }})".to_owned(),
            },
        );
        macros.insert(
            "outer".to_owned(),
            MacroDef {
                args: vec!["col".to_owned()],
                sql: "{{ inner('{{ col }}') }}".to_owned(),
            },
        );

        // Note: nested macros resolve iteratively.
        let sql = "SELECT {{ outer('name') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("LOWER(name)"));
    }

    #[test]
    fn expand_unknown_macro_preserved() {
        let macros = BTreeMap::new();
        let sql = "SELECT {{ unknown('x') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("unknown"));
    }

    #[test]
    fn expand_arg_count_mismatch() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "sk".to_owned(),
            MacroDef {
                args: vec!["a".to_owned(), "b".to_owned()],
                sql: "{{ a }} {{ b }}".to_owned(),
            },
        );

        let result = expand_macros("{{ sk('x') }}", &macros);
        assert!(result.is_err());
    }

    #[test]
    fn expand_no_macros() {
        let macros = BTreeMap::new();
        let sql = "SELECT * FROM {{ ref('t') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT * FROM {{ ref('t') }}");
    }

    #[test]
    fn expand_empty_args() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "now".to_owned(),
            MacroDef {
                args: vec![],
                sql: "CURRENT_TIMESTAMP()".to_owned(),
            },
        );

        let sql = "SELECT {{ now() }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT CURRENT_TIMESTAMP()");
    }

    // --- Additional scenarios ---

    #[test]
    fn expand_multiple_macros_in_one_sql() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "upper".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "UPPER({{ x }})".to_owned(),
            },
        );
        macros.insert(
            "lower".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "LOWER({{ x }})".to_owned(),
            },
        );

        let sql = "SELECT {{ upper('name') }}, {{ lower('email') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("UPPER(name)"));
        assert!(result.contains("LOWER(email)"));
    }

    #[test]
    fn expand_macro_with_multiple_args() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "concat".to_owned(),
            MacroDef {
                args: vec!["a".to_owned(), "b".to_owned()],
                sql: "CONCAT({{ a }}, {{ b }})".to_owned(),
            },
        );

        let sql = "SELECT {{ concat('first', 'last') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT CONCAT(first, last)");
    }

    #[test]
    fn expand_macro_preserves_config_and_ref() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "sk".to_owned(),
            MacroDef {
                args: vec!["x".to_owned()],
                sql: "MD5({{ x }})".to_owned(),
            },
        );

        let sql = "{{ config(materialized='table') }} SELECT {{ sk('id') }} FROM {{ ref('t') }} JOIN {{ source('raw', 'orders') }}";
        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("config(materialized='table')"));
        assert!(result.contains("ref('t')"));
        assert!(result.contains("source('raw', 'orders')"));
        assert!(result.contains("MD5(id)"));
    }

    #[test]
    fn expand_recursion_limit() {
        let mut macros = BTreeMap::new();
        macros.insert(
            "self_ref".to_owned(),
            MacroDef {
                args: vec![],
                sql: "{{ self_ref() }}".to_owned(),
            },
        );

        let sql = "SELECT {{ self_ref() }}";
        let result = expand_macros(sql, &macros);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("recursion"));
    }

    #[test]
    fn substitute_args_both_formats() {
        let result = substitute_args("SELECT {{ x }}, {{x}}", &["x".to_owned()], &["42"]);
        assert_eq!(result, "SELECT 42, 42");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn macro_expansion_terminates(
            body in "[A-Z]{1,50}",
        ) {
            let mut macros = BTreeMap::new();
            macros.insert("test_macro".to_owned(), MacroDef {
                args: vec!["x".to_owned()],
                sql: body,
            });
            let sql = "SELECT {{ test_macro('val') }}";
            let result = expand_macros(sql, &macros);
            prop_assert!(result.is_ok());
        }
    }
}
