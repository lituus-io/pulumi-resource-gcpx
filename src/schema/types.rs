use std::borrow::Cow;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};

/// A schema field borrowing string data from the prost_types::Struct input.
pub struct SchemaField<'a> {
    pub name: &'a str,
    pub raw_type: &'a str,
    pub canonical_type: &'static str,
    pub mode: &'a str,
    pub description: &'a str,
    pub alter: Option<AlterAction>,
    pub alter_raw: Option<&'a str>,
    pub alter_from: Option<&'a str>,
    pub default_value_expression: Option<&'a str>,
    pub rounding_mode: Option<&'a str>,
    pub fields: Vec<SchemaField<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterAction {
    Rename,
    Insert,
    Delete,
}

impl AlterAction {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("rename") {
            Some(Self::Rename)
        } else if s.eq_ignore_ascii_case("insert") {
            Some(Self::Insert)
        } else if s.eq_ignore_ascii_case("delete") {
            Some(Self::Delete)
        } else {
            None
        }
    }
}

impl fmt::Display for AlterAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rename => write!(f, "rename"),
            Self::Insert => write!(f, "insert"),
            Self::Delete => write!(f, "delete"),
        }
    }
}

/// A DDL operation borrowing column names from SchemaField.
pub enum DdlOp<'a> {
    DropColumn {
        name: &'a str,
    },
    RenameColumn {
        old_name: &'a str,
        new_name: &'a str,
    },
    AddColumn {
        name: &'a str,
        field_type: &'static str,
        mode: &'a str,
        description: &'a str,
        default_value_expression: Option<&'a str>,
        rounding_mode: Option<&'a str>,
    },
    SetDescription {
        name: &'a str,
        description: &'a str,
    },
}

/// Owned DDL operation for cycle-breaking temporary renames.
pub enum OwnedDdlOp {
    RenameColumn { old_name: String, new_name: String },
}

/// Result of compute_diff.
pub struct DiffResult<'a> {
    pub ops: Vec<DdlOp<'a>>,
    pub owned_ops: Vec<OwnedDdlOp>,
    pub replace_keys: Vec<&'static str>,
    pub has_schema_changes: bool,
}

/// A check failure — uses `Cow<'static, str>` so static validation
/// messages are zero-alloc while dynamic messages use `String`.
pub struct CheckFailure {
    pub property: Cow<'static, str>,
    pub reason: Cow<'static, str>,
}

/// Normalize BigQuery type aliases to canonical forms.
/// Zero-alloc: uses eq_ignore_ascii_case with length pre-check.
pub fn normalize_type(t: &str) -> &'static str {
    let len = t.len();
    match len {
        3 => {
            if t.eq_ignore_ascii_case("INT") {
                return "INT64";
            }
            // No 3-letter canonical types
        }
        4 => {
            if t.eq_ignore_ascii_case("BOOL") {
                return "BOOL";
            }
            if t.eq_ignore_ascii_case("DATE") {
                return "DATE";
            }
            if t.eq_ignore_ascii_case("TIME") {
                return "TIME";
            }
            if t.eq_ignore_ascii_case("JSON") {
                return "JSON";
            }
        }
        5 => {
            if t.eq_ignore_ascii_case("INT64") {
                return "INT64";
            }
            if t.eq_ignore_ascii_case("BYTES") {
                return "BYTES";
            }
            if t.eq_ignore_ascii_case("FLOAT") {
                return "FLOAT64";
            }
        }
        6 => {
            if t.eq_ignore_ascii_case("STRING") {
                return "STRING";
            }
            if t.eq_ignore_ascii_case("STRUCT") {
                return "STRUCT";
            }
            if t.eq_ignore_ascii_case("RECORD") {
                return "STRUCT";
            }
            if t.eq_ignore_ascii_case("BIGINT") {
                return "INT64";
            }
        }
        7 => {
            if t.eq_ignore_ascii_case("FLOAT64") {
                return "FLOAT64";
            }
            if t.eq_ignore_ascii_case("NUMERIC") {
                return "NUMERIC";
            }
            if t.eq_ignore_ascii_case("INTEGER") {
                return "INT64";
            }
            if t.eq_ignore_ascii_case("BOOLEAN") {
                return "BOOL";
            }
            if t.eq_ignore_ascii_case("DECIMAL") {
                return "NUMERIC";
            }
            if t.eq_ignore_ascii_case("BYTEINT") {
                return "INT64";
            }
        }
        8 => {
            if t.eq_ignore_ascii_case("DATETIME") {
                return "DATETIME";
            }
            if t.eq_ignore_ascii_case("SMALLINT") {
                return "INT64";
            }
            if t.eq_ignore_ascii_case("TINYINT") { /* len is 7 not 8 */ }
        }
        9 => {
            if t.eq_ignore_ascii_case("TIMESTAMP") {
                return "TIMESTAMP";
            }
            if t.eq_ignore_ascii_case("GEOGRAPHY") {
                return "GEOGRAPHY";
            }
        }
        10 => {
            if t.eq_ignore_ascii_case("BIGNUMERIC") {
                return "BIGNUMERIC";
            }
            if t.eq_ignore_ascii_case("BIGDECIMAL") {
                return "BIGNUMERIC";
            }
        }
        _ => {}
    }
    // Catch TINYINT (7 chars, was misplaced above)
    if t.eq_ignore_ascii_case("TINYINT") {
        return "INT64";
    }
    "UNKNOWN"
}

pub fn is_valid_type(t: &str) -> bool {
    normalize_type(t) != "UNKNOWN"
}

pub fn is_valid_mode(m: &str) -> bool {
    m.eq_ignore_ascii_case("NULLABLE")
        || m.eq_ignore_ascii_case("REQUIRED")
        || m.eq_ignore_ascii_case("REPEATED")
}

/// Column name pattern: letter or underscore, then alphanumeric or underscore, max 300.
pub fn is_valid_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 300 {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Reserved column prefixes that BigQuery does not allow.
const RESERVED_PREFIXES: &[&str] = &[
    "_TABLE_",
    "_FILE_",
    "_PARTITION",
    "_ROW_TIMESTAMP",
    "_COLIDENTIFIER",
];

/// Zero-alloc: length pre-check + eq_ignore_ascii_case on slice.
pub fn has_reserved_prefix(name: &str) -> bool {
    RESERVED_PREFIXES
        .iter()
        .any(|p| name.len() >= p.len() && name[..p.len()].eq_ignore_ascii_case(p))
}

/// Strip alter annotations from fields, producing clean fields for state storage.
pub fn clean_fields<'a>(fields: &[SchemaField<'a>]) -> Vec<SchemaField<'a>> {
    fields
        .iter()
        .filter(|f| f.alter != Some(AlterAction::Delete))
        .map(|f| SchemaField {
            name: f.name,
            raw_type: f.raw_type,
            canonical_type: f.canonical_type,
            mode: f.mode,
            description: f.description,
            alter: None,
            alter_raw: None,
            alter_from: None,
            default_value_expression: f.default_value_expression,
            rounding_mode: f.rounding_mode,
            fields: clean_fields(&f.fields),
        })
        .collect()
}

/// Compute a fingerprint hash from clean schema fields.
pub fn schema_fingerprint(fields: &[SchemaField<'_>]) -> String {
    let json = fields_to_json(fields);
    let mut h = DefaultHasher::new();
    json.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Serialize clean fields to a JSON string for resultSchemaJson output.
pub fn fields_to_json(fields: &[SchemaField<'_>]) -> String {
    let arr: Vec<serde_json::Value> = fields.iter().map(field_to_json).collect();
    serde_json::to_string(&arr).unwrap_or_default()
}

/// Convert a SchemaField to the BigQuery API JSON representation.
pub(crate) fn field_to_json(f: &SchemaField<'_>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert("name".into(), serde_json::Value::String(f.name.into()));
    m.insert(
        "type".into(),
        serde_json::Value::String(f.canonical_type.into()),
    );
    m.insert("mode".into(), serde_json::Value::String(f.mode.into()));
    if !f.description.is_empty() {
        m.insert(
            "description".into(),
            serde_json::Value::String(f.description.into()),
        );
    }
    if let Some(dve) = f.default_value_expression {
        m.insert(
            "defaultValueExpression".into(),
            serde_json::Value::String(dve.into()),
        );
    }
    if let Some(rm) = f.rounding_mode {
        m.insert("roundingMode".into(), serde_json::Value::String(rm.into()));
    }
    if !f.fields.is_empty() {
        let nested: Vec<serde_json::Value> = f.fields.iter().map(field_to_json).collect();
        m.insert("fields".into(), serde_json::Value::Array(nested));
    }
    serde_json::Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_type_canonical() {
        assert_eq!(normalize_type("STRING"), "STRING");
        assert_eq!(normalize_type("string"), "STRING");
        assert_eq!(normalize_type("Int64"), "INT64");
        assert_eq!(normalize_type("INTEGER"), "INT64");
        assert_eq!(normalize_type("INT"), "INT64");
        assert_eq!(normalize_type("SMALLINT"), "INT64");
        assert_eq!(normalize_type("BIGINT"), "INT64");
        assert_eq!(normalize_type("TINYINT"), "INT64");
        assert_eq!(normalize_type("BYTEINT"), "INT64");
        assert_eq!(normalize_type("FLOAT64"), "FLOAT64");
        assert_eq!(normalize_type("FLOAT"), "FLOAT64");
        assert_eq!(normalize_type("BOOL"), "BOOL");
        assert_eq!(normalize_type("BOOLEAN"), "BOOL");
        assert_eq!(normalize_type("STRUCT"), "STRUCT");
        assert_eq!(normalize_type("RECORD"), "STRUCT");
        assert_eq!(normalize_type("NUMERIC"), "NUMERIC");
        assert_eq!(normalize_type("DECIMAL"), "NUMERIC");
        assert_eq!(normalize_type("BIGNUMERIC"), "BIGNUMERIC");
        assert_eq!(normalize_type("BIGDECIMAL"), "BIGNUMERIC");
        assert_eq!(normalize_type("JSON"), "JSON");
        assert_eq!(normalize_type("TIMESTAMP"), "TIMESTAMP");
        assert_eq!(normalize_type("GEOGRAPHY"), "GEOGRAPHY");
    }

    #[test]
    fn normalize_type_unknown() {
        assert_eq!(normalize_type("BADTYPE"), "UNKNOWN");
        assert_eq!(normalize_type(""), "UNKNOWN");
    }

    #[test]
    fn has_reserved_prefix_case_insensitive() {
        assert!(has_reserved_prefix("_TABLE_suffix"));
        assert!(has_reserved_prefix("_table_suffix"));
        assert!(has_reserved_prefix("_PARTITION"));
        assert!(has_reserved_prefix("_partition_date"));
        assert!(!has_reserved_prefix("my_column"));
        assert!(!has_reserved_prefix("_col"));
    }

    #[test]
    fn alter_action_parse_case_insensitive() {
        assert_eq!(AlterAction::parse("rename"), Some(AlterAction::Rename));
        assert_eq!(AlterAction::parse("RENAME"), Some(AlterAction::Rename));
        assert_eq!(AlterAction::parse("Rename"), Some(AlterAction::Rename));
        assert_eq!(AlterAction::parse("INSERT"), Some(AlterAction::Insert));
        assert_eq!(AlterAction::parse("delete"), Some(AlterAction::Delete));
        assert_eq!(AlterAction::parse("invalid"), None);
    }

    #[test]
    fn is_valid_mode_case_insensitive() {
        assert!(is_valid_mode("NULLABLE"));
        assert!(is_valid_mode("nullable"));
        assert!(is_valid_mode("REQUIRED"));
        assert!(is_valid_mode("REPEATED"));
        assert!(!is_valid_mode("INVALID"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn normalize_type_idempotent(ty in "[A-Za-z0-9_]{1,20}") {
            let once = normalize_type(&ty);
            let twice = normalize_type(once);
            prop_assert_eq!(once, twice);
        }

        #[test]
        fn schema_fingerprint_deterministic(
            name in "[a-z]{1,10}",
        ) {
            let field = SchemaField {
                name: &name,
                raw_type: "STRING",
                canonical_type: "STRING",
                mode: "NULLABLE",
                description: "",
                alter: None,
                alter_raw: None,
                alter_from: None,
                default_value_expression: None,
                rounding_mode: None,
                fields: vec![],
            };
            let fp1 = schema_fingerprint(&[field]);
            let field2 = SchemaField {
                name: &name,
                raw_type: "STRING",
                canonical_type: "STRING",
                mode: "NULLABLE",
                description: "",
                alter: None,
                alter_raw: None,
                alter_from: None,
                default_value_expression: None,
                rounding_mode: None,
                fields: vec![],
            };
            let fp2 = schema_fingerprint(&[field2]);
            prop_assert_eq!(fp1, fp2);
        }
    }
}
