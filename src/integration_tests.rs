//! Mock-based integration tests — full resource lifecycle through the gRPC provider.

use std::collections::BTreeMap;

use pulumi_rs_yaml_proto::pulumirpc;
use pulumi_rs_yaml_proto::pulumirpc::resource_provider_server::ResourceProvider;
use tonic::Request;

use crate::bq::BqField;
use crate::gcp_client::MockGcpClient;
use crate::prost_util::*;
use crate::provider::GcpxProvider;

// --- Helpers ---

fn mock_provider() -> GcpxProvider<MockGcpClient> {
    GcpxProvider::new(MockGcpClient::new(vec![]))
}

fn mock_provider_with_schema(fields: Vec<BqField>) -> GcpxProvider<MockGcpClient> {
    GcpxProvider::new(MockGcpClient::new(fields))
}

fn make_field(name: &str, ty: &str) -> prost_types::Value {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_owned(), prost_string(name));
    fields.insert("type".to_owned(), prost_string(ty));
    fields.insert("mode".to_owned(), prost_string("NULLABLE"));
    prost_struct(fields)
}

fn make_schema_inputs(
    project: &str,
    dataset: &str,
    table_id: &str,
    schema: Vec<prost_types::Value>,
) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string(project)),
            ("dataset".to_owned(), prost_string(dataset)),
            ("tableId".to_owned(), prost_string(table_id)),
            ("schema".to_owned(), prost_list(schema)),
        ]
        .into_iter()
        .collect(),
    }
}

fn make_table_inputs(project: &str, dataset: &str, table_id: &str) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string(project)),
            ("dataset".to_owned(), prost_string(dataset)),
            ("tableId".to_owned(), prost_string(table_id)),
        ]
        .into_iter()
        .collect(),
    }
}

fn make_dbt_project_inputs(gcp_project: &str, dataset: &str) -> prost_types::Struct {
    let mut source_fields = BTreeMap::new();
    source_fields.insert("dataset".to_owned(), prost_string("raw_data"));
    source_fields.insert(
        "tables".to_owned(),
        prost_list(vec![prost_string("customers")]),
    );

    let mut sources = BTreeMap::new();
    sources.insert("raw".to_owned(), prost_struct(source_fields));

    prost_types::Struct {
        fields: vec![
            ("gcpProject".to_owned(), prost_string(gcp_project)),
            ("dataset".to_owned(), prost_string(dataset)),
            ("sources".to_owned(), prost_struct(sources)),
            (
                "declaredModels".to_owned(),
                prost_list(vec![prost_string("stg_customers")]),
            ),
            ("declaredMacros".to_owned(), prost_list(vec![])),
        ]
        .into_iter()
        .collect(),
    }
}

fn make_dbt_macro_inputs(name: &str, sql: &str) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("name".to_owned(), prost_string(name)),
            ("sql".to_owned(), prost_string(sql)),
            ("args".to_owned(), prost_list(vec![])),
        ]
        .into_iter()
        .collect(),
    }
}

fn make_sqljob_inputs(
    project: &str,
    region: &str,
    name: &str,
    sql: &str,
    schedule: &str,
    service_account: &str,
) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string(project)),
            ("region".to_owned(), prost_string(region)),
            ("name".to_owned(), prost_string(name)),
            ("sql".to_owned(), prost_string(sql)),
            ("schedule".to_owned(), prost_string(schedule)),
            ("timeZone".to_owned(), prost_string("UTC")),
            ("serviceAccount".to_owned(), prost_string(service_account)),
        ]
        .into_iter()
        .collect(),
    }
}

fn urn(resource_type: &str, name: &str) -> String {
    format!("urn:pulumi:stack::project::{}::{}", resource_type, name)
}

// =============================================================================
// TableSchema lifecycle
// =============================================================================

#[tokio::test]
async fn schema_full_lifecycle() {
    let p = mock_provider();

    // 1. Check
    let news = make_schema_inputs("proj", "ds", "tbl", vec![make_field("id", "INT64")]);
    let check_resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "mySchema"),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(check_resp.into_inner().failures.is_empty());

    // 2. Create
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "mySchema"),
            properties: Some(news.clone()),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let created = create_resp.into_inner();
    assert_eq!(created.id, "proj/ds/tbl");
    assert!(created.properties.is_some());

    // 3. Diff (no changes)
    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "mySchema"),
            olds: Some(news.clone()),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );

    // 4. Delete (no-op for schema)
    let del_resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "mySchema"),
            ..Default::default()
        }))
        .await;
    assert!(del_resp.is_ok());
}

#[tokio::test]
async fn schema_preview_mode() {
    let p = mock_provider();
    let news = make_schema_inputs("proj", "ds", "tbl", vec![make_field("id", "INT64")]);

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "preview"),
            properties: Some(news),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();

    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/tbl");
    // Preview should NOT execute DDL
    assert!(p.client.ddl_log().is_empty());
}

#[tokio::test]
async fn schema_update_adds_column() {
    let p = mock_provider();

    let olds = make_schema_inputs("proj", "ds", "tbl", vec![make_field("id", "INT64")]);

    // Insert a new column
    let mut insert = BTreeMap::new();
    insert.insert("name".to_owned(), prost_string("email"));
    insert.insert("type".to_owned(), prost_string("STRING"));
    insert.insert("mode".to_owned(), prost_string("NULLABLE"));
    insert.insert("alter".to_owned(), prost_string("insert"));
    let news = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![make_field("id", "INT64"), prost_struct(insert)],
    );

    // Diff should detect changes
    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds.clone()),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );

    // Update should execute DDL
    let update_resp = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(update_resp.into_inner().properties.is_some());

    let ddl_log = p.client.ddl_log();
    assert!(!ddl_log.is_empty());
    assert!(ddl_log[0].contains("ADD COLUMN"));
}

#[tokio::test]
async fn schema_read_from_bq() {
    let p = mock_provider_with_schema(vec![BqField {
        name: "id".into(),
        field_type: "INT64".into(),
        mode: "NULLABLE".into(),
        description: "".into(),
        fields: vec![],
    }]);

    let resp = p
        .read(Request::new(pulumirpc::ReadRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            ..Default::default()
        }))
        .await
        .unwrap();

    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/tbl");
    assert!(inner.properties.is_some());
    assert!(inner.inputs.is_some());
}

// =============================================================================
// Table lifecycle
// =============================================================================

#[tokio::test]
async fn table_check_valid() {
    let p = mock_provider();
    let news = make_table_inputs("proj", "ds", "my_table");

    let resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:bigquery/table:Table", "t"),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().failures.is_empty());
}

#[tokio::test]
async fn table_create_preview() {
    let p = mock_provider();
    let props = make_table_inputs("proj", "ds", "my_table");

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/table:Table", "t"),
            properties: Some(props),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/my_table");
    assert!(inner.properties.is_some());
}

#[tokio::test]
async fn table_diff_no_changes() {
    let p = mock_provider();
    let inputs = make_table_inputs("proj", "ds", "t");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/t".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn table_diff_project_change_replaces() {
    let p = mock_provider();
    let olds = make_table_inputs("proj1", "ds", "t");
    let news = make_table_inputs("proj2", "ds", "t");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj1/ds/t".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(!inner.replaces.is_empty());
    assert!(inner.replaces.contains(&"project".to_owned()));
}

#[tokio::test]
async fn table_delete_calls_api() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());

    let table_log = p.client.table_log.lock().unwrap().clone();
    assert!(!table_log.is_empty());
    assert_eq!(table_log[0].0, "delete");
}

#[tokio::test]
async fn table_read_returns_state() {
    let p = mock_provider();

    let resp = p
        .read(Request::new(pulumirpc::ReadRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            ..Default::default()
        }))
        .await
        .unwrap();

    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/tbl");
    assert!(inner.properties.is_some());
    assert!(inner.inputs.is_some());
}

#[tokio::test]
async fn table_dataset_change_replaces() {
    let p = mock_provider();
    let olds = make_table_inputs("proj", "ds1", "t");
    let news = make_table_inputs("proj", "ds2", "t");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds1/t".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.replaces.contains(&"dataset".to_owned()));
}

#[tokio::test]
async fn table_tableid_change_replaces() {
    let p = mock_provider();
    let olds = make_table_inputs("proj", "ds", "t1");
    let news = make_table_inputs("proj", "ds", "t2");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/t1".into(),
            urn: urn("gcpx:bigquery/table:Table", "t"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.replaces.contains(&"tableId".to_owned()));
}

// =============================================================================
// dbt Project lifecycle
// =============================================================================

#[tokio::test]
async fn dbt_project_check_and_create() {
    let p = mock_provider();
    let news = make_dbt_project_inputs("my-gcp-proj", "analytics");

    // Check
    let check_resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:dbt/project:Project", "proj"),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(check_resp.into_inner().failures.is_empty());

    // Create
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:dbt/project:Project", "proj"),
            properties: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = create_resp.into_inner();
    assert!(!inner.id.is_empty());
    assert_eq!(inner.id, "dbt-project/my-gcp-proj/analytics");
    assert!(inner.properties.is_some());
}

#[tokio::test]
async fn dbt_project_diff_detects_changes() {
    let p = mock_provider();
    let olds = make_dbt_project_inputs("proj1", "ds");
    let news = make_dbt_project_inputs("proj2", "ds");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "dbt-project/proj1/ds".into(),
            urn: urn("gcpx:dbt/project:Project", "proj"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

#[tokio::test]
async fn dbt_project_diff_no_changes() {
    let p = mock_provider();
    let inputs = make_dbt_project_inputs("proj", "ds");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "dbt-project/proj/ds".into(),
            urn: urn("gcpx:dbt/project:Project", "proj"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn dbt_project_update() {
    let p = mock_provider();
    let olds = make_dbt_project_inputs("proj", "ds");
    let news = make_dbt_project_inputs("proj", "new_ds");

    let resp = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "dbt-project/proj/ds".into(),
            urn: urn("gcpx:dbt/project:Project", "proj"),
            olds: Some(olds),
            news: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.properties.is_some());
    let props = inner.properties.unwrap();
    assert_eq!(get_str(&props.fields, "dataset"), Some("new_ds"));
}

// =============================================================================
// dbt Macro lifecycle
// =============================================================================

#[tokio::test]
async fn dbt_macro_check_and_create() {
    let p = mock_provider();
    let news = make_dbt_macro_inputs("my_macro", "SELECT {{ arg1 }}");

    // Check
    let check_resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:dbt/macro:Macro", "m"),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(check_resp.into_inner().failures.is_empty());

    // Create
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:dbt/macro:Macro", "m"),
            properties: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = create_resp.into_inner();
    assert_eq!(inner.id, "dbt-macro/my_macro");
    assert!(inner.properties.is_some());
}

#[tokio::test]
async fn dbt_macro_diff_no_changes() {
    let p = mock_provider();
    let inputs = make_dbt_macro_inputs("m", "SELECT 1");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "dbt-macro/m".into(),
            urn: urn("gcpx:dbt/macro:Macro", "m"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn dbt_macro_diff_detects_sql_change() {
    let p = mock_provider();
    let olds = make_dbt_macro_inputs("m", "SELECT 1");
    let news = make_dbt_macro_inputs("m", "SELECT 2");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "dbt-macro/m".into(),
            urn: urn("gcpx:dbt/macro:Macro", "m"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

// =============================================================================
// SqlJob lifecycle
// =============================================================================

#[tokio::test]
async fn sqljob_check_valid() {
    let p = mock_provider();
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "my-job",
        "SELECT 1",
        "0 * * * *",
        "sa@proj.iam.gserviceaccount.com",
    );

    let resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().failures.is_empty());
}

#[tokio::test]
async fn sqljob_check_rejects_empty_fields() {
    let p = mock_provider();
    let news = prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string("")),
            ("region".to_owned(), prost_string("")),
            ("name".to_owned(), prost_string("")),
            ("sql".to_owned(), prost_string("")),
            ("schedule".to_owned(), prost_string("")),
            ("serviceAccount".to_owned(), prost_string("")),
        ]
        .into_iter()
        .collect(),
    };

    let resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let failures = resp.into_inner().failures;
    assert!(!failures.is_empty());
    // Should flag all required empty fields
    let props: Vec<&str> = failures.iter().map(|f| f.property.as_str()).collect();
    assert!(props.contains(&"project"));
    assert!(props.contains(&"sql"));
    assert!(props.contains(&"schedule"));
}

#[tokio::test]
async fn sqljob_create_preview() {
    let p = mock_provider();
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "my-job",
        "SELECT 1",
        "0 * * * *",
        "sa@proj.iam.gserviceaccount.com",
    );

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            properties: Some(news),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "sql-job/proj/us-central1/my-job");
    assert!(inner.properties.is_some());
    // Preview should not call workflow/scheduler APIs
    assert!(p.client.workflow_log.lock().unwrap().is_empty());
    assert!(p.client.scheduler_log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sqljob_create_executes_apis() {
    let p = mock_provider();
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "my-job",
        "SELECT 1",
        "0 * * * *",
        "sa@proj.iam.gserviceaccount.com",
    );

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            properties: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "sql-job/proj/us-central1/my-job");

    // Workflow was created
    let wf_log = p.client.workflow_log.lock().unwrap().clone();
    assert!(!wf_log.is_empty());
    assert_eq!(wf_log[0].0, "create");

    // Scheduler job was created
    let sched_log = p.client.scheduler_log.lock().unwrap().clone();
    assert!(!sched_log.is_empty());
    assert_eq!(sched_log[0].0, "create");
}

#[tokio::test]
async fn sqljob_diff_no_changes() {
    let p = mock_provider();
    let inputs = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn sqljob_diff_project_change_replaces() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj1",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj2",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj1/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(inner.replaces.contains(&"project".to_owned()));
}

#[tokio::test]
async fn sqljob_diff_sql_change_updates() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 2",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    // SQL change is an update, not a replace
    assert!(inner.replaces.is_empty());
}

#[tokio::test]
async fn sqljob_update_changes_workflow() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 2",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().properties.is_some());

    // Workflow should have been updated
    let wf_log = p.client.workflow_log.lock().unwrap().clone();
    assert!(wf_log.iter().any(|(op, _, _)| op == "update"));

    // Scheduler job should have been patched
    let sched_log = p.client.scheduler_log.lock().unwrap().clone();
    assert!(sched_log.iter().any(|(op, _)| op == "patch"));
}

#[tokio::test]
async fn sqljob_delete_cleans_up() {
    let p = mock_provider();
    let props = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            properties: Some(props),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());

    // Should delete scheduler job and workflow
    let sched_log = p.client.scheduler_log.lock().unwrap().clone();
    assert!(sched_log.iter().any(|(op, _)| op == "delete"));

    let wf_log = p.client.workflow_log.lock().unwrap().clone();
    assert!(wf_log.iter().any(|(op, _, _)| op == "delete"));
}

// =============================================================================
// Schema successive updates
// =============================================================================

fn make_field_with_alter(name: &str, ty: &str, alter: &str) -> prost_types::Value {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_owned(), prost_string(name));
    fields.insert("type".to_owned(), prost_string(ty));
    fields.insert("mode".to_owned(), prost_string("NULLABLE"));
    fields.insert("alter".to_owned(), prost_string(alter));
    prost_struct(fields)
}

fn make_rename_field(new_name: &str, ty: &str, alter_from: &str) -> prost_types::Value {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_owned(), prost_string(new_name));
    fields.insert("type".to_owned(), prost_string(ty));
    fields.insert("mode".to_owned(), prost_string("NULLABLE"));
    fields.insert("alter".to_owned(), prost_string("rename"));
    fields.insert("alterFrom".to_owned(), prost_string(alter_from));
    prost_struct(fields)
}

#[tokio::test]
async fn schema_successive_updates_full_chain() {
    let p = mock_provider();

    // 1. Create with 3 cols: id, name, email
    let create_news = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_field("name", "STRING"),
            make_field("email", "STRING"),
        ],
    );
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            properties: Some(create_news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let created = create_resp.into_inner();
    assert_eq!(created.id, "proj/ds/tbl");
    let mut olds = created.properties.unwrap();

    // DDL log should be empty after create (create uses patch_table, not DDL)
    assert!(p.client.ddl_log().is_empty());

    // 2. Update 1: insert age + city
    let news1 = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_field("name", "STRING"),
            make_field("email", "STRING"),
            make_field_with_alter("age", "INT64", "insert"),
            make_field_with_alter("city", "STRING", "insert"),
        ],
    );
    let update1 = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news1),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    olds = update1.into_inner().properties.unwrap();
    let ddl_log = p.client.ddl_log();
    assert_eq!(ddl_log.len(), 1);
    assert!(ddl_log[0].contains("ADD COLUMN"));

    // 3. Update 2: rename city → city_name
    let news2 = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_field("name", "STRING"),
            make_field("email", "STRING"),
            make_field("age", "INT64"),
            make_rename_field("city_name", "STRING", "city"),
        ],
    );
    let update2 = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news2),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    olds = update2.into_inner().properties.unwrap();
    let ddl_log = p.client.ddl_log();
    assert_eq!(ddl_log.len(), 2);
    assert!(ddl_log[1].contains("RENAME COLUMN"));

    // 4. Update 3: delete email
    let news3 = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_field("name", "STRING"),
            make_field_with_alter("email", "STRING", "delete"),
            make_field("age", "INT64"),
            make_field("city_name", "STRING"),
        ],
    );
    let update3 = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news3),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    olds = update3.into_inner().properties.unwrap();
    let ddl_log = p.client.ddl_log();
    assert_eq!(ddl_log.len(), 3);
    assert!(ddl_log[2].contains("DROP COLUMN"));

    // Verify output has 4 cols (id, name, age, city_name)
    let schema_list = olds.fields.get("schema");
    assert!(schema_list.is_some());
    if let Some(prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(lv)),
    }) = schema_list
    {
        assert_eq!(lv.values.len(), 4);
    }

    // 5. Update 4: combined — +phone, name→full_name, -age
    let news4 = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_rename_field("full_name", "STRING", "name"),
            make_field_with_alter("age", "INT64", "delete"),
            make_field("city_name", "STRING"),
            make_field_with_alter("phone", "STRING", "insert"),
        ],
    );
    let update4 = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news4),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let final_props = update4.into_inner().properties.unwrap();
    let ddl_log = p.client.ddl_log();
    // 3 DDL ops (DROP, RENAME, ADD) batched into a single BEGIN...END block.
    assert_eq!(ddl_log.len(), 4);
    assert!(ddl_log[3].contains("BEGIN"));
    assert!(ddl_log[3].contains("DROP COLUMN"));
    assert!(ddl_log[3].contains("RENAME COLUMN"));
    assert!(ddl_log[3].contains("ADD COLUMN"));

    // Verify final output has 4 cols (id, full_name, city_name, phone)
    if let Some(prost_types::Value {
        kind: Some(prost_types::value::Kind::ListValue(lv)),
    }) = final_props.fields.get("schema")
    {
        assert_eq!(lv.values.len(), 4);
    }
}

#[tokio::test]
async fn schema_successive_diff_detects_changes_correctly() {
    let p = mock_provider();

    let base = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![make_field("id", "INT64"), make_field("name", "STRING")],
    );

    // Identical diff → DiffNone
    let diff_none = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(base.clone()),
            news: Some(base.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_none.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );

    // With alter annotation → DiffSome
    let with_insert = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![
            make_field("id", "INT64"),
            make_field("name", "STRING"),
            make_field_with_alter("age", "INT64", "insert"),
        ],
    );
    let diff_some = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(base.clone()),
            news: Some(with_insert),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_some.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

// =============================================================================
// Unknown resource type
// =============================================================================

#[tokio::test]
async fn unknown_resource_type_returns_not_found() {
    let p = mock_provider();
    let news = prost_types::Struct::default();

    let result = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:unknown:Foo", "x"),
            news: Some(news),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn unknown_type_diff_returns_not_found() {
    let p = mock_provider();
    let s = prost_types::Struct::default();

    let result = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "x".into(),
            urn: urn("gcpx:unknown:Foo", "x"),
            olds: Some(s.clone()),
            news: Some(s),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn unknown_type_create_returns_not_found() {
    let p = mock_provider();
    let result = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:unknown:Foo", "x"),
            properties: Some(prost_types::Struct::default()),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn unknown_type_delete_returns_not_found() {
    let p = mock_provider();
    let result = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "x".into(),
            urn: urn("gcpx:unknown:Foo", "x"),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

// =============================================================================
// Delete no-ops for metadata resources
// =============================================================================

#[tokio::test]
async fn delete_dbt_project_is_noop() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "dbt-project/proj/ds".into(),
            urn: urn("gcpx:dbt/project:Project", "proj"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn delete_dbt_macro_is_noop() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "dbt-macro/sk".into(),
            urn: urn("gcpx:dbt/macro:Macro", "sk"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn delete_table_schema_is_noop() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());
    // No DDL should have been executed
    assert!(p.client.ddl_log().is_empty());
}

// =============================================================================
// Provider-level RPCs
// =============================================================================

#[tokio::test]
async fn get_schema_returns_all_types() {
    let p = mock_provider();
    let resp = p
        .get_schema(Request::new(pulumirpc::GetSchemaRequest::default()))
        .await
        .unwrap();
    let schema = resp.into_inner().schema;
    assert!(schema.contains("gcpx:bigquery/tableSchema:TableSchema"));
    assert!(schema.contains("gcpx:bigquery/table:Table"));
    assert!(schema.contains("gcpx:dbt/project:Project"));
    assert!(schema.contains("gcpx:dbt/model:Model"));
    assert!(schema.contains("gcpx:dbt/macro:Macro"));
    assert!(schema.contains("gcpx:scheduler/sqlJob:SqlJob"));
}

#[tokio::test]
async fn configure_returns_capabilities() {
    let p = mock_provider();
    let resp = p
        .configure(Request::new(pulumirpc::ConfigureRequest::default()))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.accept_secrets);
    assert!(inner.supports_preview);
    assert!(inner.accept_resources);
}

#[tokio::test]
async fn check_config_passes_through() {
    let p = mock_provider();
    let news = prost_types::Struct::default();

    let resp = p
        .check_config(Request::new(pulumirpc::CheckRequest {
            urn: "urn:pulumi:stack::project::pulumi:providers:gcpx::default".into(),
            news: Some(news.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.failures.is_empty());
    assert_eq!(inner.inputs, Some(news));
}

#[tokio::test]
async fn cancel_succeeds() {
    let p = mock_provider();
    let resp = p.cancel(Request::new(())).await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn get_plugin_info_returns_version() {
    let p = mock_provider();
    let resp = p.get_plugin_info(Request::new(())).await.unwrap();
    let info = resp.into_inner();
    assert!(!info.version.is_empty());
}

// =============================================================================
// Cross-resource: schema update preview does not execute DDL
// =============================================================================

#[tokio::test]
async fn schema_update_preview_no_ddl() {
    let p = mock_provider();

    let olds = make_schema_inputs("proj", "ds", "tbl", vec![make_field("id", "INT64")]);

    let mut insert = BTreeMap::new();
    insert.insert("name".to_owned(), prost_string("email"));
    insert.insert("type".to_owned(), prost_string("STRING"));
    insert.insert("mode".to_owned(), prost_string("NULLABLE"));
    insert.insert("alter".to_owned(), prost_string("insert"));
    let news = make_schema_inputs(
        "proj",
        "ds",
        "tbl",
        vec![make_field("id", "INT64"), prost_struct(insert)],
    );

    let resp = p
        .update(Request::new(pulumirpc::UpdateRequest {
            id: "proj/ds/tbl".into(),
            urn: urn("gcpx:bigquery/tableSchema:TableSchema", "s"),
            olds: Some(olds),
            news: Some(news),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().properties.is_some());

    // Preview mode: DDL should NOT be executed
    assert!(p.client.ddl_log().is_empty());
}

// =============================================================================
// Table: create non-preview calls BQ API
// =============================================================================

#[tokio::test]
async fn table_create_non_preview_calls_api() {
    let p = mock_provider();
    let props = make_table_inputs("proj", "ds", "my_table");

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/table:Table", "t"),
            properties: Some(props),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/my_table");

    // create_table should have been called via the mock
    let table_log = p.client.table_log.lock().unwrap().clone();
    assert!(table_log.iter().any(|(op, _)| op == "create"));
}

// =============================================================================
// SqlJob: name change triggers replace
// =============================================================================

#[tokio::test]
async fn sqljob_diff_name_change_replaces() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj",
        "us-central1",
        "job1",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "job2",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj/us-central1/job1".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.replaces.contains(&"name".to_owned()));
}

#[tokio::test]
async fn sqljob_diff_sa_change_replaces() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa1@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa2@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert!(inner.replaces.contains(&"serviceAccount".to_owned()));
}

// =============================================================================
// SqlJob: schedule change is update, not replace
// =============================================================================

#[tokio::test]
async fn sqljob_diff_schedule_change_updates() {
    let p = mock_provider();
    let olds = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "0 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );
    let news = make_sqljob_inputs(
        "proj",
        "us-central1",
        "j",
        "SELECT 1",
        "*/5 * * * *",
        "sa@p.iam.gserviceaccount.com",
    );

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "sql-job/proj/us-central1/j".into(),
            urn: urn("gcpx:scheduler/sqlJob:SqlJob", "j"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(inner.replaces.is_empty());
}

// =============================================================================
// Dataset lifecycle
// =============================================================================

fn make_dataset_inputs(project: &str, dataset_id: &str, location: &str) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string(project)),
            ("datasetId".to_owned(), prost_string(dataset_id)),
            ("location".to_owned(), prost_string(location)),
        ]
        .into_iter()
        .collect(),
    }
}

#[tokio::test]
async fn dataset_check_valid() {
    let p = mock_provider();
    let news = make_dataset_inputs("proj", "test_ds", "US");

    let resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:bigquery/dataset:Dataset", "d"),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().failures.is_empty());
}

#[tokio::test]
async fn dataset_create_returns_id() {
    let p = mock_provider();
    let news = make_dataset_inputs("proj", "test_ds", "US");

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/dataset:Dataset", "d"),
            properties: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/test_ds");
}

#[tokio::test]
async fn dataset_diff_no_change() {
    let p = mock_provider();
    let inputs = make_dataset_inputs("proj", "test_ds", "US");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/test_ds".into(),
            urn: urn("gcpx:bigquery/dataset:Dataset", "d"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn dataset_diff_location_replaces() {
    let p = mock_provider();
    let olds = make_dataset_inputs("proj", "test_ds", "US");
    let news = make_dataset_inputs("proj", "test_ds", "EU");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/test_ds".into(),
            urn: urn("gcpx:bigquery/dataset:Dataset", "d"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(!inner.replaces.is_empty());
    assert!(inner.replaces.contains(&"location".to_owned()));
}

#[tokio::test]
async fn dataset_delete_calls_api() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "proj/test_ds".into(),
            urn: urn("gcpx:bigquery/dataset:Dataset", "d"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());

    let dataset_log = p.client.dataset_log.lock().unwrap().clone();
    assert!(!dataset_log.is_empty());
    assert_eq!(dataset_log[0].0, "delete");
}

// =============================================================================
// Routine lifecycle
// =============================================================================

fn make_routine_inputs(project: &str, dataset: &str, routine_id: &str) -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string(project)),
            ("dataset".to_owned(), prost_string(dataset)),
            ("routineId".to_owned(), prost_string(routine_id)),
            ("routineType".to_owned(), prost_string("SCALAR_FUNCTION")),
            ("language".to_owned(), prost_string("SQL")),
            ("definitionBody".to_owned(), prost_string("x + 1")),
        ]
        .into_iter()
        .collect(),
    }
}

#[tokio::test]
async fn routine_check_valid() {
    let p = mock_provider();
    let news = make_routine_inputs("proj", "ds", "my_udf");

    let resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:bigquery/routineFunction:RoutineFunction", "r"),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(resp.into_inner().failures.is_empty());
}

#[tokio::test]
async fn routine_create_returns_id() {
    let p = mock_provider();
    let news = make_routine_inputs("proj", "ds", "my_udf");

    let resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: urn("gcpx:bigquery/routineFunction:RoutineFunction", "r"),
            properties: Some(news),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(inner.id, "proj/ds/my_udf");
}

#[tokio::test]
async fn routine_diff_no_change() {
    let p = mock_provider();
    let inputs = make_routine_inputs("proj", "ds", "my_udf");

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/my_udf".into(),
            urn: urn("gcpx:bigquery/routineFunction:RoutineFunction", "r"),
            olds: Some(inputs.clone()),
            news: Some(inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn routine_diff_language_replaces() {
    let p = mock_provider();
    let olds = make_routine_inputs("proj", "ds", "my_udf");
    let mut news = make_routine_inputs("proj", "ds", "my_udf");
    news.fields
        .insert("language".to_owned(), prost_string("JAVASCRIPT"));

    let resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            id: "proj/ds/my_udf".into(),
            urn: urn("gcpx:bigquery/routineFunction:RoutineFunction", "r"),
            olds: Some(olds),
            news: Some(news),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = resp.into_inner();
    assert_eq!(
        inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(!inner.replaces.is_empty());
    assert!(inner.replaces.contains(&"language".to_owned()));
}

#[tokio::test]
async fn routine_delete_calls_api() {
    let p = mock_provider();
    let resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "proj/ds/my_udf".into(),
            urn: urn("gcpx:bigquery/routineFunction:RoutineFunction", "r"),
            ..Default::default()
        }))
        .await;
    assert!(resp.is_ok());

    let routine_log = p.client.routine_log.lock().unwrap().clone();
    assert!(!routine_log.is_empty());
    assert_eq!(routine_log[0].0, "delete");
}

// --- Dataproc IngestJob integration tests ---

fn make_ingest_inputs() -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string("proj")),
            ("region".to_owned(), prost_string("us-central1")),
            ("name".to_owned(), prost_string("my-ingest")),
            ("databaseType".to_owned(), prost_string("oracle")),
            (
                "connectionString".to_owned(),
                prost_string("jdbc:oracle:thin:@host:1521:db"),
            ),
            ("secret".to_owned(), prost_string("projects/p/secrets/s")),
            ("query".to_owned(), prost_string("SELECT * FROM users")),
            ("destination".to_owned(), prost_string("gcs")),
            ("landingBucket".to_owned(), prost_string("my-landing")),
            (
                "imageUri".to_owned(),
                prost_string("gcr.io/proj/spark:latest"),
            ),
            ("scriptUri".to_owned(), prost_string("gs://bucket/main.py")),
            (
                "serviceAccount".to_owned(),
                prost_string("sa@proj.iam.gserviceaccount.com"),
            ),
            ("stagingBucket".to_owned(), prost_string("my-staging")),
            ("processBucket".to_owned(), prost_string("my-process")),
            ("subnetworkUri".to_owned(), prost_string("subnet")),
            ("schedule".to_owned(), prost_string("0 2 * * *")),
            ("timeZone".to_owned(), prost_string("UTC")),
        ]
        .into_iter()
        .collect(),
    }
}

fn make_export_inputs() -> prost_types::Struct {
    prost_types::Struct {
        fields: vec![
            ("project".to_owned(), prost_string("proj")),
            ("region".to_owned(), prost_string("us-central1")),
            ("name".to_owned(), prost_string("my-export")),
            ("sourceDataset".to_owned(), prost_string("analytics")),
            ("sourceTable".to_owned(), prost_string("daily")),
            ("databaseType".to_owned(), prost_string("mssql")),
            (
                "connectionString".to_owned(),
                prost_string("jdbc:sqlserver://host:1433"),
            ),
            ("secret".to_owned(), prost_string("projects/p/secrets/s")),
            ("destTable".to_owned(), prost_string("test.dbo.daily")),
            ("imageUri".to_owned(), prost_string("gcr.io/img")),
            (
                "scriptUri".to_owned(),
                prost_string("gs://bucket/export.py"),
            ),
            ("serviceAccount".to_owned(), prost_string("sa@iam")),
            ("stagingBucket".to_owned(), prost_string("staging")),
            ("processBucket".to_owned(), prost_string("process")),
            ("subnetworkUri".to_owned(), prost_string("subnet")),
            ("schedule".to_owned(), prost_string("0 4 * * *")),
            ("timeZone".to_owned(), prost_string("UTC")),
        ]
        .into_iter()
        .collect(),
    }
}

#[tokio::test]
async fn test_ingest_job_full_lifecycle() {
    let p = mock_provider();
    let ingest_urn = urn("gcpx:dataproc/ingestJob:IngestJob", "test");

    // Check
    let check_resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: ingest_urn.clone(),
            news: Some(make_ingest_inputs()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(check_resp.into_inner().failures.is_empty());

    // Create preview
    let preview_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: ingest_urn.clone(),
            properties: Some(make_ingest_inputs()),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();
    let preview = preview_resp.into_inner();
    assert_eq!(preview.id, "ingest-job/proj/us-central1/my-ingest");
    assert_eq!(
        get_str(&preview.properties.as_ref().unwrap().fields, "state"),
        Some("PREVIEW")
    );

    // Create
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: ingest_urn.clone(),
            properties: Some(make_ingest_inputs()),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    let created = create_resp.into_inner();
    assert_eq!(created.id, "ingest-job/proj/us-central1/my-ingest");

    // Diff (no change)
    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            urn: ingest_urn.clone(),
            olds: Some(make_ingest_inputs()),
            news: Some(make_ingest_inputs()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );

    // Diff (query change)
    let mut changed = make_ingest_inputs();
    changed
        .fields
        .insert("query".to_owned(), prost_string("SELECT 1"));
    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            urn: ingest_urn.clone(),
            olds: Some(make_ingest_inputs()),
            news: Some(changed.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    let diff_inner = diff_resp.into_inner();
    assert_eq!(
        diff_inner.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
    assert!(diff_inner.replaces.is_empty());

    // Update
    let update_resp = p
        .update(Request::new(pulumirpc::UpdateRequest {
            urn: ingest_urn.clone(),
            olds: Some(make_ingest_inputs()),
            news: Some(changed),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(update_resp.into_inner().properties.is_some());

    // Delete
    let del_resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "ingest-job/proj/us-central1/my-ingest".into(),
            urn: ingest_urn,
            properties: Some(make_ingest_inputs()),
            ..Default::default()
        }))
        .await;
    assert!(del_resp.is_ok());
}

#[tokio::test]
async fn test_export_job_full_lifecycle() {
    let p = mock_provider();
    let export_urn = urn("gcpx:dataproc/exportJob:ExportJob", "test");

    // Check
    let check_resp = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: export_urn.clone(),
            news: Some(make_export_inputs()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(check_resp.into_inner().failures.is_empty());

    // Create preview
    let preview_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: export_urn.clone(),
            properties: Some(make_export_inputs()),
            preview: true,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        preview_resp.into_inner().id,
        "export-job/proj/us-central1/my-export"
    );

    // Create
    let create_resp = p
        .create(Request::new(pulumirpc::CreateRequest {
            urn: export_urn.clone(),
            properties: Some(make_export_inputs()),
            preview: false,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(create_resp.into_inner().properties.is_some());

    // Diff (no change)
    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            urn: export_urn.clone(),
            olds: Some(make_export_inputs()),
            news: Some(make_export_inputs()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        diff_resp.into_inner().changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );

    // Delete
    let del_resp = p
        .delete(Request::new(pulumirpc::DeleteRequest {
            id: "export-job/proj/us-central1/my-export".into(),
            urn: export_urn,
            properties: Some(make_export_inputs()),
            ..Default::default()
        }))
        .await;
    assert!(del_resp.is_ok());
}

#[tokio::test]
async fn test_ingest_job_unknown_type_returns_not_found() {
    let p = mock_provider();
    let result = p
        .check(Request::new(pulumirpc::CheckRequest {
            urn: urn("gcpx:dataproc/badType:BadType", "test"),
            news: Some(prost_types::Struct::default()),
            ..Default::default()
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_ingest_gcs_to_bq_destination_switch_replaces() {
    let p = mock_provider();
    let ingest_urn = urn("gcpx:dataproc/ingestJob:IngestJob", "test");

    let gcs_inputs = make_ingest_inputs(); // destination=gcs

    let mut bq_inputs = make_ingest_inputs();
    bq_inputs
        .fields
        .insert("destination".to_owned(), prost_string("bigquery"));
    bq_inputs.fields.remove("landingBucket");
    bq_inputs
        .fields
        .insert("destProject".to_owned(), prost_string("proj"));
    bq_inputs
        .fields
        .insert("destDataset".to_owned(), prost_string("ds"));
    bq_inputs
        .fields
        .insert("destTable".to_owned(), prost_string("tbl"));

    let diff_resp = p
        .diff(Request::new(pulumirpc::DiffRequest {
            urn: ingest_urn,
            olds: Some(gcs_inputs),
            news: Some(bq_inputs),
            ..Default::default()
        }))
        .await
        .unwrap();
    let inner = diff_resp.into_inner();
    assert!(inner.replaces.contains(&"destination".to_owned()));
}

#[tokio::test]
async fn test_provider_schema_includes_dataproc() {
    let p = mock_provider();
    let resp = p
        .get_schema(Request::new(pulumirpc::GetSchemaRequest::default()))
        .await
        .unwrap();
    let schema = resp.into_inner().schema;
    assert!(schema.contains("gcpx:dataproc/ingestJob:IngestJob"));
    assert!(schema.contains("gcpx:dataproc/exportJob:ExportJob"));
}
