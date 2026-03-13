use std::collections::BTreeMap;

// --- Read helpers (zero-copy from prost input) ---

pub fn value_as_str(v: &prost_types::Value) -> Option<&str> {
    match &v.kind {
        Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
        _ => None,
    }
}

pub fn get_str<'a>(fields: &'a BTreeMap<String, prost_types::Value>, key: &str) -> Option<&'a str> {
    fields.get(key).and_then(value_as_str)
}

pub fn get_list<'a>(
    fields: &'a BTreeMap<String, prost_types::Value>,
    key: &str,
) -> Option<&'a [prost_types::Value]> {
    fields.get(key).and_then(|v| match &v.kind {
        Some(prost_types::value::Kind::ListValue(lv)) => Some(lv.values.as_slice()),
        _ => None,
    })
}

pub fn get_bool(fields: &BTreeMap<String, prost_types::Value>, key: &str) -> Option<bool> {
    fields.get(key).and_then(|v| match &v.kind {
        Some(prost_types::value::Kind::BoolValue(b)) => Some(*b),
        _ => None,
    })
}

pub fn get_number(fields: &BTreeMap<String, prost_types::Value>, key: &str) -> Option<f64> {
    fields.get(key).and_then(|v| match &v.kind {
        Some(prost_types::value::Kind::NumberValue(n)) => Some(*n),
        _ => None,
    })
}

pub fn get_struct_fields<'a>(
    fields: &'a BTreeMap<String, prost_types::Value>,
    key: &str,
) -> Option<&'a BTreeMap<String, prost_types::Value>> {
    fields.get(key).and_then(|v| match &v.kind {
        Some(prost_types::value::Kind::StructValue(s)) => Some(&s.fields),
        _ => None,
    })
}

// --- Write helpers (owned, for gRPC output boundary) ---

pub fn prost_string(s: &str) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StringValue(s.to_owned())),
    }
}

pub fn prost_bool(b: bool) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::BoolValue(b)),
    }
}

pub fn prost_number(n: f64) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::NumberValue(n)),
    }
}

pub fn prost_list(items: Vec<prost_types::Value>) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(
            prost_types::ListValue { values: items },
        )),
    }
}

pub fn prost_struct(fields: BTreeMap<String, prost_types::Value>) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
            fields,
        })),
    }
}

pub fn prost_string_list(items: &[String]) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(
            prost_types::ListValue {
                values: items.iter().map(|s| prost_string(s)).collect(),
            },
        )),
    }
}

/// Extract a list of string references from a prost list field.
pub fn get_string_list<'a>(
    fields: &'a BTreeMap<String, prost_types::Value>,
    key: &str,
) -> Vec<&'a str> {
    get_list(fields, key)
        .map(|items| items.iter().filter_map(value_as_str).collect())
        .unwrap_or_default()
}

// --- Common ---

/// Parse a 2-part resource ID "project/resourceId" into its parts.
pub fn parse_resource_id_2(id: &str) -> Result<(&str, &str), &'static str> {
    let mut parts = id.splitn(2, '/');
    let project = parts.next().ok_or("missing project in resource id")?;
    let resource_id = parts.next().ok_or("missing resourceId in resource id")?;
    if project.is_empty() || resource_id.is_empty() {
        return Err("invalid resource id format, expected 'project/resourceId'");
    }
    Ok((project, resource_id))
}

/// Parse a 3-part resource ID "project/dataset/tableId" into its parts.
pub fn parse_resource_id(id: &str) -> Result<(&str, &str, &str), &'static str> {
    let mut parts = id.splitn(3, '/');
    let project = parts.next().ok_or("missing project in resource id")?;
    let dataset = parts.next().ok_or("missing dataset in resource id")?;
    let table_id = parts.next().ok_or("missing tableId in resource id")?;
    if project.is_empty() || dataset.is_empty() || table_id.is_empty() {
        return Err("invalid resource id format, expected 'project/dataset/tableId'");
    }
    Ok((project, dataset, table_id))
}

/// Extract the resource type from a Pulumi URN.
/// URN format: `urn:pulumi:stack::project::type::name`
pub fn resource_type_from_urn(urn: &str) -> &str {
    // Find the type segment between the 2nd and 3rd `::`.
    let mut count = 0;
    let mut start = 0;
    let bytes = urn.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            count += 1;
            if count == 2 {
                start = i + 2;
            } else if count == 3 {
                return &urn[start..i];
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    // If only 2 `::` found, the type is the rest of the string.
    if count == 2 {
        return &urn[start..];
    }
    urn
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_str_from_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("key".to_owned(), prost_string("value"));
        assert_eq!(get_str(&fields, "key"), Some("value"));
        assert_eq!(get_str(&fields, "missing"), None);
    }

    #[test]
    fn get_list_from_fields() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "items".to_owned(),
            prost_list(vec![prost_string("a"), prost_string("b")]),
        );
        let items = get_list(&fields, "items").unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn get_bool_from_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("flag".to_owned(), prost_bool(true));
        assert_eq!(get_bool(&fields, "flag"), Some(true));
    }

    #[test]
    fn get_number_from_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("count".to_owned(), prost_number(42.0));
        assert_eq!(get_number(&fields, "count"), Some(42.0));
    }

    #[test]
    fn prost_string_roundtrip() {
        let val = prost_string("hello");
        assert_eq!(value_as_str(&val), Some("hello"));
    }

    #[test]
    fn resource_type_from_urn_extracts_type() {
        let urn = "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::myres";
        assert_eq!(
            resource_type_from_urn(urn),
            "gcpx:bigquery/tableSchema:TableSchema"
        );
    }

    #[test]
    fn parse_resource_id_roundtrip() {
        let (p, d, t) = parse_resource_id("proj/ds/tbl").unwrap();
        assert_eq!(p, "proj");
        assert_eq!(d, "ds");
        assert_eq!(t, "tbl");
    }

    #[test]
    fn parse_resource_id_invalid() {
        assert!(parse_resource_id("invalid").is_err());
        assert!(parse_resource_id("a/b").is_err());
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parse_resource_id_proptest_roundtrip(
            project in "[a-z][a-z0-9-]{2,19}",
            dataset in "[a-z][a-z0-9_]{2,19}",
            table in "[a-z][a-z0-9_]{2,19}",
        ) {
            let id = format!("{project}/{dataset}/{table}");
            let (p, d, t) = parse_resource_id(&id).unwrap();
            prop_assert_eq!(p, project.as_str());
            prop_assert_eq!(d, dataset.as_str());
            prop_assert_eq!(t, table.as_str());
        }
    }
}
