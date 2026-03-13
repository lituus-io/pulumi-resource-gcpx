use std::collections::BTreeMap;

use crate::bq::BqField;
use crate::prost_util::{get_list, get_str, prost_string, prost_string_list};
use crate::schema::types::{
    fields_to_json, normalize_type, schema_fingerprint, AlterAction, SchemaField,
};

/// Parse a single schema field from a prost_types::Value (recursive for STRUCT/RECORD).
pub fn parse_field(value: &prost_types::Value) -> Option<SchemaField<'_>> {
    let fields = match &value.kind {
        Some(prost_types::value::Kind::StructValue(s)) => &s.fields,
        _ => return None,
    };

    let name = get_str(fields, "name")?;
    let raw_type = get_str(fields, "type").unwrap_or("STRING");
    let canonical_type = normalize_type(raw_type);
    let mode = get_str(fields, "mode").unwrap_or("NULLABLE");
    let description = get_str(fields, "description").unwrap_or("");
    let alter_raw = get_str(fields, "alter");
    let alter = alter_raw.and_then(|s| {
        if s.is_empty() {
            None
        } else {
            AlterAction::parse(s)
        }
    });
    let alter_from = get_str(fields, "alterFrom");
    let default_value_expression = get_str(fields, "defaultValueExpression");
    let rounding_mode = get_str(fields, "roundingMode");

    let nested = get_list(fields, "fields")
        .map(|items| items.iter().filter_map(parse_field).collect())
        .unwrap_or_default();

    Some(SchemaField {
        name,
        raw_type,
        canonical_type,
        mode,
        description,
        alter: if alter_raw.is_some_and(|s| !s.is_empty()) {
            alter
        } else {
            None
        },
        alter_raw: if alter_raw.is_some_and(|s| !s.is_empty()) {
            alter_raw
        } else {
            None
        },
        alter_from: if alter_from.is_some_and(|s| !s.is_empty()) {
            alter_from
        } else {
            None
        },
        default_value_expression: if default_value_expression.is_some_and(|s| !s.is_empty()) {
            default_value_expression
        } else {
            None
        },
        rounding_mode: if rounding_mode.is_some_and(|s| !s.is_empty()) {
            rounding_mode
        } else {
            None
        },
        fields: nested,
    })
}

/// Parse the full inputs struct into (project, dataset, table_id, fields).
pub fn parse_schema_inputs(
    s: &prost_types::Struct,
) -> Result<(&str, &str, &str, Vec<SchemaField<'_>>), &'static str> {
    let project = get_str(&s.fields, "project").ok_or("missing 'project'")?;
    let dataset = get_str(&s.fields, "dataset").ok_or("missing 'dataset'")?;
    let table_id = get_str(&s.fields, "tableId").ok_or("missing 'tableId'")?;
    let schema_list = get_list(&s.fields, "schema").ok_or("missing 'schema'")?;

    let fields: Vec<SchemaField<'_>> = schema_list.iter().filter_map(parse_field).collect();

    Ok((project, dataset, table_id, fields))
}

/// Convert a single SchemaField to a prost StructValue (recursive).
fn field_to_prost(f: &SchemaField<'_>) -> prost_types::Value {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_owned(), prost_string(f.name));
    fields.insert("type".to_owned(), prost_string(f.canonical_type));
    fields.insert("mode".to_owned(), prost_string(f.mode));
    if !f.description.is_empty() {
        fields.insert("description".to_owned(), prost_string(f.description));
    }
    if let Some(dve) = f.default_value_expression {
        fields.insert("defaultValueExpression".to_owned(), prost_string(dve));
    }
    if let Some(rm) = f.rounding_mode {
        fields.insert("roundingMode".to_owned(), prost_string(rm));
    }
    if !f.fields.is_empty() {
        fields.insert("fields".to_owned(), fields_to_prost_list(&f.fields));
    }
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
            fields,
        })),
    }
}

/// Convert SchemaField slice to prost ListValue.
fn fields_to_prost_list(fields: &[SchemaField<'_>]) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(
            prost_types::ListValue {
                values: fields.iter().map(field_to_prost).collect(),
            },
        )),
    }
}

/// Build clean output prost_types::Struct from borrowed fields.
pub fn build_schema_output(
    project: &str,
    dataset: &str,
    table_id: &str,
    clean_fields: &[SchemaField<'_>],
    pending_ddl: &[String],
) -> prost_types::Struct {
    let mut fields = BTreeMap::new();
    fields.insert("project".to_owned(), prost_string(project));
    fields.insert("dataset".to_owned(), prost_string(dataset));
    fields.insert("tableId".to_owned(), prost_string(table_id));
    fields.insert("schema".to_owned(), fields_to_prost_list(clean_fields));

    let json = fields_to_json(clean_fields);
    fields.insert("resultSchemaJson".to_owned(), prost_string(&json));
    fields.insert(
        "schemaFingerprint".to_owned(),
        prost_string(&schema_fingerprint(clean_fields)),
    );

    if !pending_ddl.is_empty() {
        fields.insert("pendingDdl".to_owned(), prost_string_list(pending_ddl));
    }

    prost_types::Struct { fields }
}

/// Build output prost_types::Struct from BqField (owned API response).
pub fn build_output_from_bq_fields(
    project: &str,
    dataset: &str,
    table_id: &str,
    bq_fields: &[BqField],
) -> prost_types::Struct {
    let schema_fields: Vec<SchemaField<'_>> = bq_fields.iter().map(bq_field_to_schema).collect();
    build_schema_output(project, dataset, table_id, &schema_fields, &[])
}

fn bq_field_to_schema(bf: &BqField) -> SchemaField<'_> {
    SchemaField {
        name: &bf.name,
        raw_type: &bf.field_type,
        canonical_type: normalize_type(&bf.field_type),
        mode: if bf.mode.is_empty() {
            "NULLABLE"
        } else {
            &bf.mode
        },
        description: &bf.description,
        alter: None,
        alter_raw: None,
        alter_from: None,
        default_value_expression: None,
        rounding_mode: None,
        fields: bf.fields.iter().map(bq_field_to_schema).collect(),
    }
}
