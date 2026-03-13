//! Real GCP integration tests.
//!
//! Run with:
//! ```
//! GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
//!   cargo test --test gcp_integration -- --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pulumi_resource_gcpx::bq::BqOps;
use pulumi_resource_gcpx::gcp_client::HttpGcpClient;
use pulumi_resource_gcpx::scheduler_ops::SchedulerOps;
use pulumi_resource_gcpx::token_source::GcpAuthTokenSource;

const TEST_PROJECT: &str = "my-gcp-project";
const TEST_DATASET: &str = "gcpx_test";
const TEST_REGION: &str = "northamerica-northeast1";

type TestClient = HttpGcpClient<GcpAuthTokenSource>;

static COUNTER: AtomicU64 = AtomicU64::new(0);

async fn gcp_client() -> TestClient {
    let token_source = GcpAuthTokenSource(
        gcp_auth::provider()
            .await
            .expect("GCP auth failed — set GOOGLE_APPLICATION_CREDENTIALS"),
    );
    HttpGcpClient::new(reqwest::Client::new(), token_source)
}

/// Generate unique name using timestamp + atomic counter to avoid collisions.
fn unique_name(prefix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, ts, c)
}

async fn platform_token() -> String {
    let provider = gcp_auth::provider().await.unwrap();
    let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
    provider.token(scopes).await.unwrap().as_str().to_owned()
}

/// Ensure the test dataset exists. Creates it if missing.
async fn ensure_dataset(_client: &TestClient) {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
        TEST_PROJECT
    );
    let body = serde_json::json!({
        "datasetReference": {
            "projectId": TEST_PROJECT,
            "datasetId": TEST_DATASET,
        },
        "location": "US",
    });
    let token = platform_token().await;
    let http = reqwest::Client::new();
    let resp = http
        .post(&url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    if status != 200 && status != 409 {
        let text = resp.text().await.unwrap_or_default();
        panic!("Failed to create dataset: {} {}", status, text);
    }
}

/// Ensure Cloud Scheduler API is enabled and SA has the right role. Idempotent.
async fn ensure_scheduler_api() {
    let token = platform_token().await;
    let http = reqwest::Client::new();

    // 1. Enable Cloud Scheduler API.
    let url = format!(
        "https://serviceusage.googleapis.com/v1/projects/{}/services/cloudscheduler.googleapis.com:enable",
        TEST_PROJECT
    );
    let resp = http
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    if status != 200 && status != 409 {
        let text = resp.text().await.unwrap_or_default();
        eprintln!("Warning: Could not enable Cloud Scheduler API ({status}): {text}");
    }

    // 2. Grant roles/cloudscheduler.admin to the service account.
    let sa_member = format!(
        "serviceAccount:terraform@{}.iam.gserviceaccount.com",
        TEST_PROJECT
    );
    let get_url = format!(
        "https://cloudresourcemanager.googleapis.com/v1/projects/{}:getIamPolicy",
        TEST_PROJECT
    );
    let resp = http
        .post(&get_url)
        .bearer_auth(&token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    if resp.status().is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        if let Ok(mut policy) = serde_json::from_str::<serde_json::Value>(&body_text) {
            let bindings = policy["bindings"].as_array().cloned().unwrap_or_default();
            let has_role = bindings.iter().any(|b| {
                b["role"].as_str() == Some("roles/cloudscheduler.admin")
                    && b["members"]
                        .as_array()
                        .map(|m| m.iter().any(|v| v.as_str() == Some(&sa_member)))
                        .unwrap_or(false)
            });

            if !has_role {
                let mut new_bindings = bindings;
                new_bindings.push(serde_json::json!({
                    "role": "roles/cloudscheduler.admin",
                    "members": [&sa_member],
                }));
                policy["bindings"] = serde_json::Value::Array(new_bindings);

                let set_url = format!(
                    "https://cloudresourcemanager.googleapis.com/v1/projects/{}:setIamPolicy",
                    TEST_PROJECT
                );
                let set_resp = http
                    .post(&set_url)
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "policy": policy }))
                    .send()
                    .await
                    .unwrap();
                let set_status = set_resp.status();
                if !set_status.is_success() {
                    let text = set_resp.text().await.unwrap_or_default();
                    eprintln!("Warning: Could not set IAM policy ({set_status}): {text}");
                } else {
                    eprintln!(
                        "Granted roles/cloudscheduler.admin to SA. Waiting for propagation..."
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            }
        }
    }

    // Give the API a moment to fully activate.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
}

/// Helper: wait for a workflow to become ACTIVE.
async fn wait_workflow_active(client: &TestClient, name: &str) {
    for _ in 0..30 {
        match client.get_workflow(TEST_PROJECT, TEST_REGION, name).await {
            Ok(wf) if wf.state == "ACTIVE" => return,
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
    panic!("workflow '{}' did not become ACTIVE after 60s", name);
}

fn sa() -> String {
    format!("terraform@{}.iam.gserviceaccount.com", TEST_PROJECT)
}

/// Helper: create scheduler job with retry for 409 "sync mutate calls cannot be queued".
async fn create_scheduler_job_with_retry(
    client: &TestClient,
    body: &serde_json::Value,
) -> pulumi_resource_gcpx::scheduler_ops::SchedulerJobMeta {
    for attempt in 0..5 {
        match client
            .create_scheduler_job(TEST_PROJECT, TEST_REGION, body)
            .await
        {
            Ok(meta) => return meta,
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("409") && attempt < 4 {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
                panic!("create_scheduler_job failed after retries: {e}");
            }
        }
    }
    unreachable!()
}

// =========================================================================
// BigQuery Schema Evolution (BqOps::execute_ddl + get_table_schema)
// =========================================================================

#[tokio::test]
async fn test_gcp_schema_add_column() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("add_col");

    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (id INT64, name STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();

    let alter_ddl = format!(
        "ALTER TABLE `{}.{}.{}` ADD COLUMN email STRING",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &alter_ddl, None)
        .await
        .unwrap();

    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 3);
    assert!(schema.iter().any(|f| f.name == "email"));

    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

#[tokio::test]
async fn test_gcp_schema_rename_column() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("rename_col");

    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (old_name STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();

    let rename_ddl = format!(
        "ALTER TABLE `{}.{}.{}` RENAME COLUMN old_name TO new_name",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &rename_ddl, None)
        .await
        .unwrap();

    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert!(schema.iter().any(|f| f.name == "new_name"));
    assert!(!schema.iter().any(|f| f.name == "old_name"));

    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

#[tokio::test]
async fn test_gcp_schema_drop_column() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("drop_col");

    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (id INT64, to_drop STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();

    let drop_col = format!(
        "ALTER TABLE `{}.{}.{}` DROP COLUMN to_drop",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &drop_col, None)
        .await
        .unwrap();

    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "id");

    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

#[tokio::test]
async fn test_gcp_schema_set_description() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("set_desc");

    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (id INT64, name STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();

    let alter_ddl = format!(
        "ALTER TABLE `{}.{}.{}` ALTER COLUMN name SET OPTIONS (description='Customer name')",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &alter_ddl, None)
        .await
        .unwrap();

    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    let name_field = schema.iter().find(|f| f.name == "name").unwrap();
    assert_eq!(name_field.description, "Customer name");

    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

// =========================================================================
// BigQuery Table CRUD (BqOps::create_table/get_table/patch_table/delete_table)
// =========================================================================

#[tokio::test]
async fn test_gcp_create_and_delete_table() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("crud");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [{"name": "id", "type": "INT64"}] },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.schema_fields.len(), 1);
    assert_eq!(meta.schema_fields[0].name, "id");
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.creation_time > 0);

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();

    // Verify deleted — get_table should fail.
    let err = client.get_table(TEST_PROJECT, TEST_DATASET, &table).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_gcp_create_view() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let view = unique_name("view");

    let body = serde_json::json!({
        "tableReference": { "tableId": &view },
        "view": {
            "query": "SELECT 1 AS id, 'hello' AS name",
            "useLegacySql": false,
        },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &view)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "VIEW");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &view)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_patch_table_description() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("patch_desc");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [{"name": "id", "type": "INT64"}] },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let patch = serde_json::json!({ "description": "Updated description" });
    client
        .patch_table(TEST_PROJECT, TEST_DATASET, &table, &patch)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.description, "Updated description");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_patch_table_friendly_name() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("patch_fname");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [{"name": "id", "type": "INT64"}] },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let patch = serde_json::json!({ "friendlyName": "My Friendly Table" });
    client
        .patch_table(TEST_PROJECT, TEST_DATASET, &table, &patch)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.friendly_name, "My Friendly Table");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_patch_table_labels() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("patch_labels");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [{"name": "id", "type": "INT64"}] },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let patch = serde_json::json!({
        "labels": { "env": "test", "team": "data" },
    });
    client
        .patch_table(TEST_PROJECT, TEST_DATASET, &table, &patch)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.labels.get("env").map(|s| s.as_str()), Some("test"));
    assert_eq!(meta.labels.get("team").map(|s| s.as_str()), Some("data"));

    // Update labels — change one, remove one.
    let patch2 = serde_json::json!({
        "labels": { "env": "prod" },
    });
    client
        .patch_table(TEST_PROJECT, TEST_DATASET, &table, &patch2)
        .await
        .unwrap();
    let meta2 = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta2.labels.get("env").map(|s| s.as_str()), Some("prod"));

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_table_with_time_partitioning() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("partition");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [
            {"name": "id", "type": "INT64"},
            {"name": "ts", "type": "TIMESTAMP"},
        ]},
        "timePartitioning": {
            "type": "DAY",
            "field": "ts",
        },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert_eq!(meta.schema_fields.len(), 2);

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_table_with_clustering() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("cluster");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [
            {"name": "id", "type": "INT64"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "category", "type": "STRING"},
        ]},
        "timePartitioning": {
            "type": "DAY",
            "field": "ts",
        },
        "clustering": {
            "fields": ["category"],
        },
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert_eq!(meta.schema_fields.len(), 3);

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_table_with_multiple_columns_and_types() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("multi_col");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [
            {"name": "id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING"},
            {"name": "amount", "type": "FLOAT64"},
            {"name": "active", "type": "BOOL"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "metadata", "type": "STRING", "mode": "REPEATED"},
        ]},
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(meta.schema_fields.len(), 6);

    let id_field = meta.schema_fields.iter().find(|f| f.name == "id").unwrap();
    assert_eq!(id_field.field_type, "INTEGER");
    assert_eq!(id_field.mode, "REQUIRED");

    let name_field = meta
        .schema_fields
        .iter()
        .find(|f| f.name == "name")
        .unwrap();
    assert_eq!(name_field.field_type, "STRING");

    let metadata_field = meta
        .schema_fields
        .iter()
        .find(|f| f.name == "metadata")
        .unwrap();
    assert_eq!(metadata_field.mode, "REPEATED");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_table_nested_struct() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("nested");

    let body = serde_json::json!({
        "tableReference": { "tableId": &table },
        "schema": { "fields": [
            {"name": "id", "type": "INT64"},
            {"name": "address", "type": "RECORD", "fields": [
                {"name": "street", "type": "STRING"},
                {"name": "city", "type": "STRING"},
                {"name": "zip", "type": "STRING"},
            ]},
        ]},
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 2);
    let addr = schema.iter().find(|f| f.name == "address").unwrap();
    assert_eq!(addr.field_type, "RECORD");
    assert_eq!(addr.fields.len(), 3);
    assert!(addr.fields.iter().any(|f| f.name == "street"));

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
}

// =========================================================================
// BigQuery Dry-Run (BqOps::dry_run_query)
// =========================================================================

#[tokio::test]
async fn test_gcp_dry_run_valid_query() {
    let client = gcp_client().await;

    let result = client
        .dry_run_query(TEST_PROJECT, "SELECT 1 AS id", None)
        .await
        .unwrap();
    assert!(result.valid);
    assert!(result.error_message.is_none());
}

#[tokio::test]
async fn test_gcp_dry_run_invalid_query() {
    let client = gcp_client().await;

    let result = client
        .dry_run_query(TEST_PROJECT, "SELECT FROM WHERE INVALID SYNTAX", None)
        .await
        .unwrap();
    assert!(!result.valid);
    assert!(result.error_message.is_some());
}

#[tokio::test]
async fn test_gcp_dry_run_complex_query() {
    let client = gcp_client().await;

    let sql = "WITH cte AS (SELECT 1 AS id, 'hello' AS name) SELECT id, UPPER(name) FROM cte";
    let result = client.dry_run_query(TEST_PROJECT, sql, None).await.unwrap();
    assert!(result.valid);
}

// =========================================================================
// dbt End-to-End (resolve → generate_ddl → execute → verify)
// =========================================================================

#[tokio::test]
async fn test_gcp_dbt_create_view_model() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let view_name = unique_name("dbt_view");

    let sql = "{{ config(materialized='view') }}\nSELECT 1 AS id, 'hello' AS name";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();
    let macros = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &view_name,
        "view",
        &resolved,
        None,
        None,
        None,
    );
    assert!(ddl.starts_with("CREATE OR REPLACE VIEW"));

    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &view_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "VIEW");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &view_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dbt_create_table_model() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("dbt_table");

    let sql = "{{ config(materialized='table') }}\nSELECT 1 AS id, CURRENT_TIMESTAMP() AS ts";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();
    let macros = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    assert!(ddl.starts_with("CREATE OR REPLACE TABLE"));

    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.num_rows >= 0);

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dbt_source_resolution() {
    use pulumi_resource_gcpx::dbt::resolver::resolve;
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec!["customers".to_owned()],
        },
    );

    let sql = "{{ config(materialized='view') }}\nSELECT * FROM {{ source('raw', 'customers') }}";
    let refs = BTreeMap::new();
    let macros = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let full_sql = resolved.to_sql();
    assert!(full_sql.contains(&format!("`{}.{}.customers`", TEST_PROJECT, TEST_DATASET)));
}

#[tokio::test]
async fn test_gcp_dbt_non_ephemeral_ref_resolution() {
    use pulumi_resource_gcpx::dbt::resolver::resolve;
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let mut refs = BTreeMap::new();
    refs.insert(
        "stg_users".to_owned(),
        ModelRefData {
            materialization: "view".to_owned(),
            resolved_ctes_json: String::new(),
            resolved_body: String::new(),
            table_ref: format!("`{}.{}.stg_users`", TEST_PROJECT, TEST_DATASET),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    let macros = BTreeMap::new();

    let sql = "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('stg_users') }}";
    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let full_sql = resolved.to_sql();
    assert!(full_sql.contains(&format!("`{}.{}.stg_users`", TEST_PROJECT, TEST_DATASET)));
    assert!(resolved.ctes.is_empty());
}

#[tokio::test]
async fn test_gcp_dbt_ephemeral_cte_inlining() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("dbt_eph");

    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let mut refs = BTreeMap::new();
    refs.insert(
        "stg".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: "SELECT 1 AS id, 'Alice' AS name".to_owned(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    let macros = BTreeMap::new();

    let sql = "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('stg') }}";
    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();

    // Verify CTE flattening happened.
    assert_eq!(resolved.ctes.len(), 1);
    assert_eq!(resolved.ctes[0].0, "__dbt__cte__stg");
    assert!(resolved.body.contains("__dbt__cte__stg"));

    let full_sql = resolved.to_sql();
    assert!(full_sql.starts_with("WITH __dbt__cte__stg AS ("));

    // Actually execute it.
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    // BQ DDL with CTEs may need a moment to propagate.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dbt_transitive_ephemeral_chain() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("dbt_chain");

    // Chain: model_a (ephemeral) -> model_b (ephemeral) -> final (table)
    // model_a: no upstream
    // model_b: refs model_a (so its ctes include __dbt__cte__model_a)
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let mut refs = BTreeMap::new();

    // model_a is ephemeral with no upstream CTEs.
    refs.insert(
        "model_a".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: "SELECT 1 AS id".to_owned(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );

    // model_b is ephemeral, depends on model_a. Its resolved_ctes_json carries model_a's CTE.
    refs.insert(
        "model_b".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: serde_json::to_string(&vec![(
                "__dbt__cte__model_a".to_owned(),
                "SELECT 1 AS id".to_owned(),
            )])
            .unwrap(),
            resolved_body: "SELECT id, id * 2 AS doubled FROM __dbt__cte__model_a".to_owned(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    let macros = BTreeMap::new();

    let sql = "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('model_b') }}";
    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();

    // Should have 2 CTEs: __dbt__cte__model_a, then __dbt__cte__model_b.
    assert_eq!(resolved.ctes.len(), 2);
    assert_eq!(resolved.ctes[0].0, "__dbt__cte__model_a");
    assert_eq!(resolved.ctes[1].0, "__dbt__cte__model_b");

    // Execute and verify.
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dbt_macro_expansion_and_execute() {
    use pulumi_resource_gcpx::dbt::macros::MacroDef;
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("dbt_macro");

    let mut macros = BTreeMap::new();
    macros.insert(
        "upper_col".to_owned(),
        MacroDef {
            args: vec!["col".to_owned()],
            sql: "UPPER({{ col }})".to_owned(),
        },
    );

    let sql =
        "{{ config(materialized='table') }}\nSELECT {{ upper_col('\"hello\"') }} AS val, 1 AS id";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    assert!(resolved.body.contains("UPPER(\"hello\")"));

    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dbt_nested_macros_and_execute() {
    use pulumi_resource_gcpx::dbt::macros::MacroDef;
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("dbt_nested");

    let mut macros = BTreeMap::new();
    macros.insert(
        "wrap".to_owned(),
        MacroDef {
            args: vec!["x".to_owned()],
            sql: "CONCAT('[', {{ x }}, ']')".to_owned(),
        },
    );
    macros.insert(
        "upper_wrap".to_owned(),
        MacroDef {
            args: vec!["col".to_owned()],
            sql: "{{ wrap('UPPER({{ col }})') }}".to_owned(),
        },
    );

    let sql = "{{ config(materialized='table') }}\nSELECT {{ upper_wrap('\"test\"') }} AS val";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );

    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dry_run_resolved_dbt_sql() {
    use pulumi_resource_gcpx::dbt::resolver::resolve;
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;

    let sql = "{{ config(materialized='table') }}\nSELECT 1 AS id, 'test' AS name";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();
    let macros = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let full_sql = resolved.to_sql();

    let result = client
        .dry_run_query(TEST_PROJECT, &full_sql, None)
        .await
        .unwrap();
    assert!(result.valid);
}

#[tokio::test]
async fn test_gcp_dbt_ephemeral_no_ddl() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let sql = "{{ config(materialized='ephemeral') }}\nSELECT 1 AS id";
    let sources: BTreeMap<String, SourceDef> = BTreeMap::new();
    let refs = BTreeMap::new();
    let macros = BTreeMap::new();

    let resolved = resolve(sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        "eph",
        "ephemeral",
        &resolved,
        None,
        None,
        None,
    );
    assert!(ddl.is_empty());
}

#[tokio::test]
async fn test_gcp_dbt_source_ref_combined() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    // Create a source table first.
    let src_table = unique_name("dbt_src");
    let body = serde_json::json!({
        "tableReference": { "tableId": &src_table },
        "schema": { "fields": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "STRING"},
        ]},
    });
    client
        .create_table(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();

    let view_name = unique_name("dbt_combined");

    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec![src_table.clone()],
        },
    );

    let mut refs = BTreeMap::new();
    refs.insert(
        "stg_helper".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: format!(
                "SELECT id, UPPER(name) AS name FROM `{}.{}.{}`",
                TEST_PROJECT, TEST_DATASET, src_table
            ),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    let macros = BTreeMap::new();

    let sql = format!(
        "{{{{ config(materialized='view') }}}}\nSELECT h.* FROM {{{{ ref('stg_helper') }}}} h JOIN {{{{ source('raw', '{}') }}}} r ON h.id = r.id",
        src_table
    );

    let resolved = resolve(&sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    assert_eq!(resolved.ctes.len(), 1); // ephemeral CTE
    let full_sql = resolved.to_sql();
    assert!(full_sql.contains("__dbt__cte__stg_helper"));
    assert!(full_sql.contains(&format!(
        "`{}.{}.{}`",
        TEST_PROJECT, TEST_DATASET, src_table
    )));

    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &view_name,
        "view",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &view_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "VIEW");

    // Cleanup.
    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &view_name)
        .await
        .unwrap();
    client
        .delete_table(TEST_PROJECT, TEST_DATASET, &src_table)
        .await
        .unwrap();
}

// =========================================================================
// Workflow YAML generation
// =========================================================================

#[tokio::test]
async fn test_gcp_workflow_yaml_valid() {
    use pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml;

    let sql = "SELECT 1 AS id";
    let yaml = generate_workflow_yaml(TEST_PROJECT, sql);

    assert!(yaml.contains("googleapis.bigquery.v2.jobs.query"));
    assert!(yaml.contains(TEST_PROJECT));
    assert!(yaml.contains(sql));
}

#[tokio::test]
async fn test_gcp_workflow_yaml_multiline() {
    use pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml;

    let sql = "CREATE OR REPLACE TABLE `p.d.t` AS\nSELECT *\nFROM source\nWHERE active = TRUE";
    let yaml = generate_workflow_yaml(TEST_PROJECT, sql);

    assert!(yaml.contains("CREATE OR REPLACE TABLE"));
    assert!(yaml.contains("FROM source"));
    assert!(yaml.contains("WHERE active = TRUE"));
}

// =========================================================================
// Cloud Scheduler + Workflows (SchedulerOps)
// =========================================================================

#[tokio::test]
async fn test_gcp_create_and_delete_workflow() {
    let client = gcp_client().await;
    let wf_name = unique_name("wf");

    let definition = "- step1:\n    call: sys.log\n    args:\n        text: hello";
    let meta = client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, definition, &sa())
        .await
        .unwrap();
    assert!(!meta.name.is_empty());

    wait_workflow_active(&client, &wf_name).await;

    // Verify get_workflow returns correct data.
    let wf = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    assert_eq!(wf.state, "ACTIVE");
    assert!(!wf.revision_id.is_empty());

    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();

    // Workflow deletion is async (LRO). Wait until get returns error or non-ACTIVE state.
    for _ in 0..15 {
        match client
            .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
            .await
        {
            Err(_) => return,                         // Fully deleted.
            Ok(wf) if wf.state != "ACTIVE" => return, // Deleting.
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
    // After 30s, it should at least be in a non-ACTIVE state.
    let result = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await;
    assert!(result.is_err() || result.unwrap().state != "ACTIVE");
}

#[tokio::test]
async fn test_gcp_update_workflow() {
    let client = gcp_client().await;
    let wf_name = unique_name("wf_upd");

    let definition1 = "- step1:\n    call: sys.log\n    args:\n        text: version1";
    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, definition1, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf1 = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    let rev1 = wf1.revision_id.clone();

    // Update the workflow definition.
    let definition2 = "- step1:\n    call: sys.log\n    args:\n        text: version2";
    client
        .update_workflow(TEST_PROJECT, TEST_REGION, &wf_name, definition2)
        .await
        .unwrap();

    wait_workflow_active(&client, &wf_name).await;

    let wf2 = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    // Revision ID should change after update.
    assert_ne!(wf2.revision_id, rev1);
    assert_eq!(wf2.state, "ACTIVE");

    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_create_and_delete_scheduler_job() {
    let client = gcp_client().await;
    ensure_scheduler_api().await;
    let job_name = unique_name("sched");

    let body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", TEST_PROJECT, TEST_REGION, job_name),
        "schedule": "0 0 1 1 *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": "https://httpbin.org/post",
            "httpMethod": "POST",
        },
    });

    let meta = create_scheduler_job_with_retry(&client, &body).await;
    assert!(!meta.name.is_empty());
    assert_eq!(meta.schedule, "0 0 1 1 *");
    assert_eq!(meta.time_zone, "UTC");

    let fetched = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name)
        .await
        .unwrap();
    assert!(fetched.state == "ENABLED" || fetched.state == "ACTIVE");

    // Small delay to avoid 409 "sync mutate calls cannot be queued".
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name)
        .await
        .unwrap();

    // Verify deleted.
    let err = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name)
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_gcp_patch_scheduler_job() {
    let client = gcp_client().await;
    ensure_scheduler_api().await;
    let job_name = unique_name("sched_patch");

    let body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", TEST_PROJECT, TEST_REGION, job_name),
        "schedule": "0 0 1 1 *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": "https://httpbin.org/post",
            "httpMethod": "POST",
        },
    });
    create_scheduler_job_with_retry(&client, &body).await;

    // Patch the schedule.
    let patch = serde_json::json!({
        "schedule": "0 5 * * *",
        "timeZone": "America/Toronto",
        "httpTarget": {
            "uri": "https://httpbin.org/post",
            "httpMethod": "POST",
        },
    });
    let patched = client
        .patch_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name, &patch)
        .await
        .unwrap();
    assert_eq!(patched.schedule, "0 5 * * *");
    assert_eq!(patched.time_zone, "America/Toronto");

    // Verify via get.
    let fetched = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name)
        .await
        .unwrap();
    assert_eq!(fetched.schedule, "0 5 * * *");

    client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, &job_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_scheduler_full_lifecycle() {
    use pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml;

    let client = gcp_client().await;
    ensure_scheduler_api().await;
    let base_name = unique_name("lifecycle");
    let wf_name = format!("gcpx-wf-{}", base_name);
    let sched_name = format!("gcpx-sched-{}", base_name);

    // 1. Create workflow with BQ SQL.
    let yaml = generate_workflow_yaml(TEST_PROJECT, "SELECT 1");
    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    // 2. Create scheduler job targeting the workflow.
    let wf_full = format!(
        "projects/{}/locations/{}/workflows/{}",
        TEST_PROJECT, TEST_REGION, wf_name
    );
    let body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", TEST_PROJECT, TEST_REGION, sched_name),
        "schedule": "0 0 1 1 *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": format!("https://workflowexecutions.googleapis.com/v1/{}/executions", wf_full),
            "httpMethod": "POST",
            "oauthToken": {
                "serviceAccountEmail": sa(),
                "scope": "https://www.googleapis.com/auth/cloud-platform",
            },
        },
    });
    create_scheduler_job_with_retry(&client, &body).await;

    // 3. Verify scheduler job.
    let sched = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    assert!(sched.state == "ENABLED" || sched.state == "ACTIVE");

    // 4. Update the workflow SQL.
    let yaml2 = generate_workflow_yaml(TEST_PROJECT, "SELECT 2");
    client
        .update_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml2)
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    // 5. Cleanup: delete scheduler first, then workflow.
    client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
}

// =========================================================================
// Error Paths
// =========================================================================

#[tokio::test]
async fn test_gcp_get_nonexistent_table() {
    let client = gcp_client().await;
    let result = client
        .get_table(TEST_PROJECT, TEST_DATASET, "nonexistent_table_xyz")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_gcp_delete_nonexistent_table() {
    let client = gcp_client().await;
    // delete_ok treats 404 as success (idempotent delete).
    let result = client
        .delete_table(TEST_PROJECT, TEST_DATASET, "nonexistent_table_xyz")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_gcp_get_nonexistent_workflow() {
    let client = gcp_client().await;
    let result = client
        .get_workflow(TEST_PROJECT, TEST_REGION, "nonexistent_workflow_xyz")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_gcp_delete_nonexistent_workflow_no_error() {
    let client = gcp_client().await;
    // delete_workflow ignores 404.
    let result = client
        .delete_workflow(TEST_PROJECT, TEST_REGION, "nonexistent_workflow_xyz")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_gcp_get_nonexistent_scheduler_job() {
    let client = gcp_client().await;
    let result = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, "nonexistent_job_xyz")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_gcp_delete_nonexistent_scheduler_no_error() {
    let client = gcp_client().await;
    ensure_scheduler_api().await;
    // delete_scheduler_job ignores 404.
    let result = client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, "nonexistent_job_xyz")
        .await;
    assert!(result.is_ok());
}

// =========================================================================
// dbt Multi-Model Pipeline Tests
// =========================================================================

#[tokio::test]
async fn test_gcp_dbt_multi_model_pipeline() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    let src_table = unique_name("src_pipe");
    let stg_view = unique_name("stg_pipe");
    let mart_table = unique_name("mart_pipe");

    // 1. Create source table with data.
    let create_src =
        format!(
        "CREATE TABLE `{p}.{d}.{t}` AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'",
        p = TEST_PROJECT, d = TEST_DATASET, t = src_table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_src, None)
        .await
        .unwrap();

    // 2. stg_model (view) reads from source.
    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec![src_table.clone()],
        },
    );

    let stg_sql = format!(
        "{{{{ config(materialized='view') }}}}\nSELECT id, name FROM {{{{ source('raw', '{}') }}}}",
        src_table
    );
    let resolved_stg = resolve(
        &stg_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &sources,
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let stg_ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &stg_view,
        "view",
        &resolved_stg,
        None,
        None,
        None,
    );
    client
        .execute_ddl(TEST_PROJECT, &stg_ddl, None)
        .await
        .unwrap();

    // 3. mart_model (table) reads from stg via ref.
    let stg_resolved_sql = resolved_stg.to_sql();
    let mut refs = BTreeMap::new();
    refs.insert(
        stg_view.clone(),
        ModelRefData {
            materialization: "view".to_owned(),
            resolved_ctes_json: serde_json::to_string(&resolved_stg.ctes).unwrap(),
            resolved_body: resolved_stg.body.clone(),
            table_ref: format!("`{}.{}.{}`", TEST_PROJECT, TEST_DATASET, stg_view),
            resolved_ddl: stg_ddl.clone(),
            resolved_sql: stg_resolved_sql.into_owned(),
            workflow_yaml: String::new(),
        },
    );

    let mart_sql = format!(
        "{{{{ config(materialized='table') }}}}\nSELECT id, UPPER(name) AS name FROM {{{{ ref('{}') }}}}",
        stg_view
    );
    let resolved_mart = resolve(
        &mart_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &sources,
        &refs,
        &BTreeMap::new(),
    )
    .unwrap();
    let mart_ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &mart_table,
        "table",
        &resolved_mart,
        None,
        None,
        None,
    );
    client
        .execute_ddl(TEST_PROJECT, &mart_ddl, None)
        .await
        .unwrap();

    // 4. Verify mart_model.
    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &mart_table)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.schema_fields.iter().any(|f| f.name == "id"));
    assert!(meta.schema_fields.iter().any(|f| f.name == "name"));

    // 5. Cleanup in reverse order.
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &mart_table)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &stg_view)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &src_table)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_multi_model_three_levels() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::ModelRefData;

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    let raw_stg = unique_name("raw_stg");
    let mart_final = unique_name("mart_final");

    // 1. raw_stg (view) — simple SELECT from literal data.
    let stg_sql = "{{ config(materialized='view') }}\nSELECT 1 AS id, 'hello' AS val";
    let resolved_stg = resolve(
        stg_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let stg_ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &raw_stg,
        "view",
        &resolved_stg,
        None,
        None,
        None,
    );
    client
        .execute_ddl(TEST_PROJECT, &stg_ddl, None)
        .await
        .unwrap();

    // 2. int_enriched (ephemeral) — refs raw_stg, adds computed column.
    let stg_resolved_sql = resolved_stg.to_sql();
    let mut refs_for_int = BTreeMap::new();
    refs_for_int.insert(
        raw_stg.clone(),
        ModelRefData {
            materialization: "view".to_owned(),
            resolved_ctes_json: serde_json::to_string(&resolved_stg.ctes).unwrap(),
            resolved_body: resolved_stg.body.clone(),
            table_ref: format!("`{}.{}.{}`", TEST_PROJECT, TEST_DATASET, raw_stg),
            resolved_ddl: stg_ddl.clone(),
            resolved_sql: stg_resolved_sql.into_owned(),
            workflow_yaml: String::new(),
        },
    );

    let int_sql = format!(
        "{{{{ config(materialized='ephemeral') }}}}\nSELECT id, UPPER(val) AS val FROM {{{{ ref('{}') }}}}",
        raw_stg
    );
    let resolved_int = resolve(
        &int_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &refs_for_int,
        &BTreeMap::new(),
    )
    .unwrap();
    // ephemeral generates no DDL.
    let int_ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        "int_enriched",
        "ephemeral",
        &resolved_int,
        None,
        None,
        None,
    );
    assert!(int_ddl.is_empty());

    // 3. mart_final (table) — refs int_enriched (ephemeral CTE inlining).
    let mut refs_for_mart = BTreeMap::new();
    refs_for_mart.insert(
        "int_enriched".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: serde_json::to_string(&resolved_int.ctes).unwrap(),
            resolved_body: resolved_int.body.clone(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: resolved_int.to_sql().into_owned(),
            workflow_yaml: String::new(),
        },
    );

    let mart_sql = "{{ config(materialized='table') }}\nSELECT * FROM {{ ref('int_enriched') }}";
    let resolved_mart = resolve(
        mart_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &refs_for_mart,
        &BTreeMap::new(),
    )
    .unwrap();
    let mart_ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &mart_final,
        "table",
        &resolved_mart,
        None,
        None,
        None,
    );

    // Verify CTE chain is in DDL.
    assert!(mart_ddl.contains("WITH"));
    assert!(mart_ddl.contains("__dbt__cte__int_enriched"));

    client
        .execute_ddl(TEST_PROJECT, &mart_ddl, None)
        .await
        .unwrap();

    // 4. Verify mart_final.
    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &mart_final)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.schema_fields.iter().any(|f| f.name == "id"));
    assert!(meta.schema_fields.iter().any(|f| f.name == "val"));

    // 5. Cleanup.
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &mart_final)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &raw_stg)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_multiple_sources() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::SourceDef;

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    let src_a = unique_name("src_a");
    let src_b = unique_name("src_b");
    let result_table = unique_name("join_result");

    // Create 2 source tables.
    let create_a = format!(
        "CREATE TABLE `{p}.{d}.{t}` AS SELECT 1 AS id, 'Alice' AS name",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = src_a
    );
    let create_b = format!(
        "CREATE TABLE `{p}.{d}.{t}` AS SELECT 1 AS id, 100 AS amount",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = src_b
    );
    client
        .execute_ddl(TEST_PROJECT, &create_a, None)
        .await
        .unwrap();
    client
        .execute_ddl(TEST_PROJECT, &create_b, None)
        .await
        .unwrap();

    // Model JOINs both sources.
    let mut sources = BTreeMap::new();
    sources.insert(
        "alpha".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec![src_a.clone()],
        },
    );
    sources.insert(
        "beta".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec![src_b.clone()],
        },
    );

    let sql = format!(
        "{{{{ config(materialized='table') }}}}\nSELECT a.id, a.name, b.amount FROM {{{{ source('alpha', '{}') }}}} a JOIN {{{{ source('beta', '{}') }}}} b ON a.id = b.id",
        src_a, src_b
    );
    let resolved = resolve(
        &sql,
        TEST_PROJECT,
        TEST_DATASET,
        &sources,
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &result_table,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    // Verify.
    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &result_table)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.schema_fields.iter().any(|f| f.name == "amount"));

    // Cleanup.
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &result_table)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &src_a)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &src_b)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_multiple_macros_real() {
    use pulumi_resource_gcpx::dbt::macros::MacroDef;
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("multi_macro");

    let mut macros = BTreeMap::new();
    macros.insert(
        "upper_col".to_owned(),
        MacroDef {
            args: vec!["x".to_owned()],
            sql: "UPPER({{ x }})".to_owned(),
        },
    );
    macros.insert(
        "concat_cols".to_owned(),
        MacroDef {
            args: vec!["a".to_owned(), "b".to_owned()],
            sql: "CONCAT({{ a }}, ' ', {{ b }})".to_owned(),
        },
    );

    let sql = "{{ config(materialized='table') }}\nSELECT {{ upper_col('col1') }} AS upper_val, {{ concat_cols('col1', 'col2') }} AS full_name FROM (SELECT 'hello' AS col1, 'world' AS col2)";
    let resolved = resolve(
        sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &macros,
    )
    .unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );

    assert!(ddl.contains("UPPER(col1)"));
    assert!(ddl.contains("CONCAT(col1, ' ', col2)"));

    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");
    assert!(meta.schema_fields.iter().any(|f| f.name == "upper_val"));
    assert!(meta.schema_fields.iter().any(|f| f.name == "full_name"));

    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_model_with_macros_sources_refs() {
    use pulumi_resource_gcpx::dbt::macros::MacroDef;
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::dbt::types::{ModelRefData, SourceDef};

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    let src_table = unique_name("combo_src");
    let result_table = unique_name("combo_result");

    // Create source.
    let create_src = format!(
        "CREATE TABLE `{p}.{d}.{t}` AS SELECT 1 AS id, 'alice' AS name",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = src_table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_src, None)
        .await
        .unwrap();

    // Set up sources.
    let mut sources = BTreeMap::new();
    sources.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: TEST_DATASET.to_owned(),
            tables: vec![src_table.clone()],
        },
    );

    // Set up ephemeral ref.
    let eph_sql = "{{ config(materialized='ephemeral') }}\nSELECT 42 AS magic_number";
    let resolved_eph = resolve(
        eph_sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let mut refs = BTreeMap::new();
    refs.insert(
        "magic".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: serde_json::to_string(&resolved_eph.ctes).unwrap(),
            resolved_body: resolved_eph.body.clone(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: resolved_eph.to_sql().into_owned(),
            workflow_yaml: String::new(),
        },
    );

    // Set up macros.
    let mut macros = BTreeMap::new();
    macros.insert(
        "upper_col".to_owned(),
        MacroDef {
            args: vec!["x".to_owned()],
            sql: "UPPER({{ x }})".to_owned(),
        },
    );

    // Model uses source + ref + macro.
    let sql = format!(
        "{{{{ config(materialized='table') }}}}\nSELECT s.id, {{{{ upper_col('s.name') }}}} AS name, m.magic_number FROM {{{{ source('raw', '{}') }}}} s CROSS JOIN {{{{ ref('magic') }}}} m",
        src_table
    );
    let resolved = resolve(&sql, TEST_PROJECT, TEST_DATASET, &sources, &refs, &macros).unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &result_table,
        "table",
        &resolved,
        None,
        None,
        None,
    );

    assert!(ddl.contains("UPPER(s.name)"));
    assert!(ddl.contains("__dbt__cte__magic"));

    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &result_table)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    // Cleanup.
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &result_table)
        .await;
    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &src_table)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_workflow_yaml_from_resolved_model() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};
    use pulumi_resource_gcpx::scheduler::workflow_template::generate_workflow_yaml;

    let client = gcp_client().await;
    ensure_dataset(&client).await;

    let table_name = unique_name("wf_model");

    let sql = "{{ config(materialized='table') }}\nSELECT 1 AS id, 'workflow_test' AS val";
    let resolved = resolve(
        sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    let yaml = generate_workflow_yaml(TEST_PROJECT, &ddl);

    // Verify YAML structure.
    assert!(yaml.contains("runQuery"));
    assert!(yaml.contains("googleapis.bigquery.v2.jobs.query"));
    assert!(yaml.contains(TEST_PROJECT));
    assert!(yaml.contains("CREATE OR REPLACE TABLE"));

    // Execute DDL directly (not via workflow — workflow creation is slow).
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    let meta = client
        .get_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(meta.table_type, "TABLE");

    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await;
}

// =========================================================================
// dbt Validation Error Actionability (no GCP needed)
// =========================================================================

#[tokio::test]
async fn test_gcp_dbt_validation_errors_actionable() {
    use pulumi_resource_gcpx::dbt::types::{ProjectContext, SourceDef};
    use pulumi_resource_gcpx::dbt::validate::validate_model;

    let default_config = pulumi_resource_gcpx::dbt::types::ModelConfig {
        materialization: "table".to_owned(),
        unique_key: None,
        unique_key_list: None,
        incremental_strategy: None,
        partition_by: None,
        cluster_by: None,
        require_partition_filter: None,
        partition_expiration_days: None,
        friendly_name: None,
        description: None,
        labels: vec![],
        kms_key_name: None,
        default_collation_name: None,
        enable_refresh: None,
        refresh_interval_minutes: None,
        max_staleness: None,
    };

    let mut sources = BTreeMap::new();
    sources.insert(
        "staging".to_owned(),
        SourceDef {
            dataset: "stg".to_owned(),
            tables: vec!["orders".to_owned()],
        },
    );
    let ctx = ProjectContext {
        gcp_project: "p",
        dataset: "d",
        sources,
        declared_models: vec!["m".to_owned(), "stg".to_owned()],
        declared_macros: vec![],
        vars: BTreeMap::new(),
    };

    // Unknown ref → message lists available models.
    let failures = validate_model(
        "m",
        "{{ config(materialized='table') }} SELECT * FROM {{ ref('missing') }}",
        &default_config,
        &ctx,
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert!(failures
        .iter()
        .any(|f| f.reason.contains("stg") && f.reason.contains("references unknown model")));

    // Unknown source → message lists available sources.
    let failures = validate_model(
        "m",
        "{{ config(materialized='table') }} SELECT * FROM {{ source('unknown', 'tbl') }}",
        &default_config,
        &ctx,
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert!(failures
        .iter()
        .any(|f| f.reason.contains("staging") && f.reason.contains("unknown source")));

    // No config block is now allowed — defaults to table materialization.
    let failures = validate_model(
        "m",
        "SELECT 1",
        &default_config,
        &ctx,
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert!(
        failures.is_empty(),
        "expected no failures for SQL without config: {:?}",
        failures.iter().map(|f| &f.reason).collect::<Vec<_>>()
    );

    // Incremental without unique_key → message requires unique_key.
    let incr_sql = "{{ config(materialized='incremental') }} SELECT 1";
    let incr_config =
        pulumi_resource_gcpx::dbt::handlers::extract_model_config(incr_sql, &BTreeMap::new());
    let failures = validate_model(
        "m",
        incr_sql,
        &incr_config,
        &ctx,
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert!(failures.iter().any(|f| { f.reason.contains("unique_key") }));
}

#[tokio::test]
async fn test_gcp_dbt_table_model_verify_data() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table_name = unique_name("verify_data");

    let sql = "{{ config(materialized='table') }}\nSELECT 42 AS answer, 'hello' AS greeting";
    let resolved = resolve(
        sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table_name,
        "table",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    // Verify schema. BQ REST API returns "INTEGER" (not "INT64").
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table_name)
        .await
        .unwrap();
    assert_eq!(schema.len(), 2);
    assert!(schema
        .iter()
        .any(|f| f.name == "answer" && f.field_type == "INTEGER"));
    assert!(schema
        .iter()
        .any(|f| f.name == "greeting" && f.field_type == "STRING"));

    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &table_name)
        .await;
}

#[tokio::test]
async fn test_gcp_dbt_view_model_verify_schema() {
    use pulumi_resource_gcpx::dbt::resolver::{generate_ddl, resolve};

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let view_name = unique_name("verify_schema");

    let sql = "{{ config(materialized='view') }}\nSELECT CAST(1 AS INT64) AS id, CAST('test' AS STRING) AS name, CAST(3.14 AS FLOAT64) AS score";
    let resolved = resolve(
        sql,
        TEST_PROJECT,
        TEST_DATASET,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
    .unwrap();
    let ddl = generate_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &view_name,
        "view",
        &resolved,
        None,
        None,
        None,
    );
    client.execute_ddl(TEST_PROJECT, &ddl, None).await.unwrap();

    // BQ REST API returns "INTEGER" and "FLOAT" (not "INT64"/"FLOAT64").
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &view_name)
        .await
        .unwrap();
    assert_eq!(schema.len(), 3);
    assert!(schema
        .iter()
        .any(|f| f.name == "id" && f.field_type == "INTEGER"));
    assert!(schema
        .iter()
        .any(|f| f.name == "name" && f.field_type == "STRING"));
    assert!(schema
        .iter()
        .any(|f| f.name == "score" && f.field_type == "FLOAT"));

    let _ = client
        .delete_table(TEST_PROJECT, TEST_DATASET, &view_name)
        .await;
}

// =========================================================================
// BigQuery Dataset CRUD (BqOps::create_dataset/get_dataset/patch_dataset/delete_dataset)
// =========================================================================

#[tokio::test]
async fn test_gcp_dataset_create_and_delete() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let dataset_id = unique_name("ds_crud");

    let body = serde_json::json!({
        "datasetReference": {
            "projectId": TEST_PROJECT,
            "datasetId": &dataset_id,
        },
        "location": "US",
        "description": "Integration test dataset",
    });
    let meta = client.create_dataset(TEST_PROJECT, &body).await.unwrap();
    assert_eq!(meta.dataset_id, dataset_id);
    assert!(meta.creation_time > 0);

    // Verify via get.
    let fetched = client.get_dataset(TEST_PROJECT, &dataset_id).await.unwrap();
    assert_eq!(fetched.dataset_id, dataset_id);
    assert_eq!(fetched.description, "Integration test dataset");

    // Delete.
    client
        .delete_dataset(TEST_PROJECT, &dataset_id)
        .await
        .unwrap();

    // Verify deleted — get_dataset should fail.
    let err = client.get_dataset(TEST_PROJECT, &dataset_id).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_gcp_dataset_update_description() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let dataset_id = unique_name("ds_patch");

    let body = serde_json::json!({
        "datasetReference": {
            "projectId": TEST_PROJECT,
            "datasetId": &dataset_id,
        },
        "location": "US",
        "description": "Original description",
    });
    client.create_dataset(TEST_PROJECT, &body).await.unwrap();

    // Patch description.
    let patch = serde_json::json!({
        "description": "Updated description",
    });
    let patched = client
        .patch_dataset(TEST_PROJECT, &dataset_id, &patch)
        .await
        .unwrap();
    assert_eq!(patched.description, "Updated description");

    // Verify via get.
    let fetched = client.get_dataset(TEST_PROJECT, &dataset_id).await.unwrap();
    assert_eq!(fetched.description, "Updated description");

    // Cleanup.
    client
        .delete_dataset(TEST_PROJECT, &dataset_id)
        .await
        .unwrap();
}

// =========================================================================
// BigQuery Routine CRUD (BqOps::create_routine/get_routine/delete_routine)
// =========================================================================

#[tokio::test]
async fn test_gcp_create_sql_udf_and_delete() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let routine_id = unique_name("udf");

    let body = serde_json::json!({
        "routineReference": {
            "projectId": TEST_PROJECT,
            "datasetId": TEST_DATASET,
            "routineId": &routine_id,
        },
        "routineType": "SCALAR_FUNCTION",
        "language": "SQL",
        "definitionBody": "x + 1",
        "arguments": [
            {
                "name": "x",
                "dataType": { "typeKind": "INT64" },
            }
        ],
        "returnType": { "typeKind": "INT64" },
    });
    let meta = client
        .create_routine(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();
    assert_eq!(meta.routine_id, routine_id);
    assert_eq!(meta.routine_type, "SCALAR_FUNCTION");
    assert_eq!(meta.language, "SQL");
    assert!(meta.creation_time > 0);

    // Verify via get.
    let fetched = client
        .get_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await
        .unwrap();
    assert_eq!(fetched.routine_id, routine_id);
    assert_eq!(fetched.routine_type, "SCALAR_FUNCTION");

    // Delete.
    client
        .delete_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await
        .unwrap();

    // Verify deleted — get_routine should fail.
    let err = client
        .get_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_gcp_create_stored_procedure_and_delete() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let routine_id = unique_name("proc");

    let body = serde_json::json!({
        "routineReference": {
            "projectId": TEST_PROJECT,
            "datasetId": TEST_DATASET,
            "routineId": &routine_id,
        },
        "routineType": "PROCEDURE",
        "language": "SQL",
        "definitionBody": "BEGIN SELECT 1; END;",
    });
    let meta = client
        .create_routine(TEST_PROJECT, TEST_DATASET, &body)
        .await
        .unwrap();
    assert_eq!(meta.routine_id, routine_id);
    assert_eq!(meta.routine_type, "PROCEDURE");
    assert_eq!(meta.language, "SQL");
    assert!(meta.creation_time > 0);

    // Verify via get.
    let fetched = client
        .get_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await
        .unwrap();
    assert_eq!(fetched.routine_id, routine_id);
    assert_eq!(fetched.routine_type, "PROCEDURE");

    // Delete.
    client
        .delete_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await
        .unwrap();

    // Verify deleted — get_routine should fail.
    let err = client
        .get_routine(TEST_PROJECT, TEST_DATASET, &routine_id)
        .await;
    assert!(err.is_err());
}

// =========================================================================
// Schema Successive Evolution (raw DDL)
// =========================================================================

#[tokio::test]
async fn test_gcp_schema_successive_evolution() {
    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("evo");

    // 1. CREATE TABLE with 3 cols: id, name, email
    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (id INT64, name STRING, email STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 3);

    // 2. ADD 2 cols: age, city
    let add_ddl = format!(
        "ALTER TABLE `{p}.{d}.{t}` ADD COLUMN IF NOT EXISTS `age` INT64, ADD COLUMN IF NOT EXISTS `city` STRING",
        p = TEST_PROJECT, d = TEST_DATASET, t = table
    );
    client
        .execute_ddl(TEST_PROJECT, &add_ddl, None)
        .await
        .unwrap();
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 5);
    assert!(schema.iter().any(|f| f.name == "age"));
    assert!(schema.iter().any(|f| f.name == "city"));

    // 3. RENAME city → city_name
    let rename_ddl = format!(
        "ALTER TABLE `{p}.{d}.{t}` RENAME COLUMN `city` TO `city_name`",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = table
    );
    client
        .execute_ddl(TEST_PROJECT, &rename_ddl, None)
        .await
        .unwrap();
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 5);
    assert!(schema.iter().any(|f| f.name == "city_name"));
    assert!(!schema.iter().any(|f| f.name == "city"));

    // 4. DROP email
    let drop_ddl = format!(
        "ALTER TABLE `{p}.{d}.{t}` DROP COLUMN IF EXISTS `email`",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = table
    );
    client
        .execute_ddl(TEST_PROJECT, &drop_ddl, None)
        .await
        .unwrap();
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 4);
    assert!(!schema.iter().any(|f| f.name == "email"));

    // 5. Combined: DROP age + RENAME name → full_name + ADD phone
    let drop2 = format!(
        "ALTER TABLE `{p}.{d}.{t}` DROP COLUMN IF EXISTS `age`",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = table
    );
    client
        .execute_ddl(TEST_PROJECT, &drop2, None)
        .await
        .unwrap();
    let rename2 = format!(
        "ALTER TABLE `{p}.{d}.{t}` RENAME COLUMN `name` TO `full_name`",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = table
    );
    client
        .execute_ddl(TEST_PROJECT, &rename2, None)
        .await
        .unwrap();
    let add2 = format!(
        "ALTER TABLE `{p}.{d}.{t}` ADD COLUMN IF NOT EXISTS `phone` STRING",
        p = TEST_PROJECT,
        d = TEST_DATASET,
        t = table
    );
    client.execute_ddl(TEST_PROJECT, &add2, None).await.unwrap();

    // Final: 4 cols (id, full_name, city_name, phone)
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 4);
    let names: Vec<&str> = schema.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"full_name"));
    assert!(names.contains(&"city_name"));
    assert!(names.contains(&"phone"));

    // Cleanup
    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

#[tokio::test]
async fn test_gcp_schema_successive_via_diff_pipeline() {
    use pulumi_resource_gcpx::schema::ddl::build_batch_ddl;
    use pulumi_resource_gcpx::schema::diff::compute_diff;
    use pulumi_resource_gcpx::schema::types::{
        clean_fields, normalize_type, AlterAction, SchemaField,
    };

    let client = gcp_client().await;
    ensure_dataset(&client).await;
    let table = unique_name("diff_pipe");

    fn mk_field<'a>(name: &'a str, ty: &'a str) -> SchemaField<'a> {
        SchemaField {
            name,
            raw_type: ty,
            canonical_type: normalize_type(ty),
            mode: "NULLABLE",
            description: "",
            alter: None,
            alter_raw: None,
            alter_from: None,
            default_value_expression: None,
            rounding_mode: None,
            fields: vec![],
        }
    }

    // 1. CREATE TABLE with 3 cols
    let create_ddl = format!(
        "CREATE TABLE `{}.{}.{}` (id INT64, name STRING, email STRING)",
        TEST_PROJECT, TEST_DATASET, table
    );
    client
        .execute_ddl(TEST_PROJECT, &create_ddl, None)
        .await
        .unwrap();

    let step0 = vec![
        mk_field("id", "INT64"),
        mk_field("name", "STRING"),
        mk_field("email", "STRING"),
    ];

    // 2. Insert age + city via diff pipeline
    let mut age = mk_field("age", "INT64");
    age.alter = Some(AlterAction::Insert);
    let mut city = mk_field("city", "STRING");
    city.alter = Some(AlterAction::Insert);
    let step1_news = vec![
        mk_field("id", "INT64"),
        mk_field("name", "STRING"),
        mk_field("email", "STRING"),
        age,
        city,
    ];
    let diff1 = compute_diff("p", "p", "d", "d", "t", "t", &step0, &step1_news);
    let stmts1 = build_batch_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table,
        &diff1.ops,
        &diff1.owned_ops,
    );
    for stmt in &stmts1 {
        client.execute_ddl(TEST_PROJECT, stmt, None).await.unwrap();
    }
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 5);

    let step1_clean = clean_fields(&step1_news);

    // 3. Rename city → city_name via diff pipeline
    let mut city_name = mk_field("city_name", "STRING");
    city_name.alter = Some(AlterAction::Rename);
    city_name.alter_from = Some("city");
    let step2_news = vec![
        mk_field("id", "INT64"),
        mk_field("name", "STRING"),
        mk_field("email", "STRING"),
        mk_field("age", "INT64"),
        city_name,
    ];
    let diff2 = compute_diff("p", "p", "d", "d", "t", "t", &step1_clean, &step2_news);
    let stmts2 = build_batch_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table,
        &diff2.ops,
        &diff2.owned_ops,
    );
    for stmt in &stmts2 {
        client.execute_ddl(TEST_PROJECT, stmt, None).await.unwrap();
    }
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert!(schema.iter().any(|f| f.name == "city_name"));
    assert!(!schema.iter().any(|f| f.name == "city"));

    let step2_clean = clean_fields(&step2_news);

    // 4. Drop email via diff pipeline
    let mut del_email = mk_field("email", "STRING");
    del_email.alter = Some(AlterAction::Delete);
    let step3_news = vec![
        mk_field("id", "INT64"),
        mk_field("name", "STRING"),
        del_email,
        mk_field("age", "INT64"),
        mk_field("city_name", "STRING"),
    ];
    let diff3 = compute_diff("p", "p", "d", "d", "t", "t", &step2_clean, &step3_news);
    let stmts3 = build_batch_ddl(
        TEST_PROJECT,
        TEST_DATASET,
        &table,
        &diff3.ops,
        &diff3.owned_ops,
    );
    for stmt in &stmts3 {
        client.execute_ddl(TEST_PROJECT, stmt, None).await.unwrap();
    }
    let schema = client
        .get_table_schema(TEST_PROJECT, TEST_DATASET, &table)
        .await
        .unwrap();
    assert_eq!(schema.len(), 4);
    assert!(!schema.iter().any(|f| f.name == "email"));

    // Final: id, name, age, city_name
    let names: Vec<&str> = schema.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"name"));
    assert!(names.contains(&"age"));
    assert!(names.contains(&"city_name"));

    // Cleanup
    let drop = format!("DROP TABLE `{}.{}.{}`", TEST_PROJECT, TEST_DATASET, table);
    let _ = client.execute_ddl(TEST_PROJECT, &drop, None).await;
}

// =========================================================================
// Dataproc Workflow Template (via SchedulerOps)
// =========================================================================

#[tokio::test]
async fn test_gcp_dataproc_ingest_workflow_create_and_delete() {
    use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

    let client = gcp_client().await;
    let wf_name = unique_name("dp_ingest");

    let yaml = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        "test-ingest",
        "gcr.io/proj/spark:latest",
        "gs://bucket/main.py",
        &[],
        &["arg1", "arg2"],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        false,
    );

    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    assert_eq!(wf.state, "ACTIVE");

    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();

    // Wait for deletion.
    for _ in 0..15 {
        match client
            .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
            .await
        {
            Err(_) => return,
            Ok(wf) if wf.state != "ACTIVE" => return,
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
    let result = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await;
    assert!(result.is_err() || result.unwrap().state != "ACTIVE");
}

#[tokio::test]
async fn test_gcp_dataproc_ingest_workflow_update() {
    use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

    let client = gcp_client().await;
    let wf_name = unique_name("dp_upd");

    let yaml1 = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        "test-upd",
        "gcr.io/proj/spark:latest",
        "gs://bucket/main.py",
        &[],
        &["v1"],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        false,
    );
    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml1, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf1 = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    let rev1 = wf1.revision_id.clone();

    let yaml2 = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        "test-upd",
        "gcr.io/proj/spark:latest",
        "gs://bucket/main.py",
        &[],
        &["v2", "extra"],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        true,
    );
    client
        .update_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml2)
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf2 = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    assert_ne!(wf2.revision_id, rev1);
    assert_eq!(wf2.state, "ACTIVE");

    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dataproc_ingest_scheduler_full_lifecycle() {
    use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

    let client = gcp_client().await;
    ensure_scheduler_api().await;
    let base_name = unique_name("dp_lc");
    let wf_name = format!("gcpx-wf-ingest-{}", base_name);
    let sched_name = format!("gcpx-sched-ingest-{}", base_name);

    // 1. Create workflow with Dataproc template.
    let yaml = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        &base_name,
        "gcr.io/proj/spark:latest",
        "gs://bucket/main.py",
        &[],
        &[
            "secret",
            "gs://landing",
            "SELECT 1",
            "jdbc:oracle:thin:@host:1521:db",
            "process",
        ],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        false,
    );
    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    // 2. Create scheduler targeting the workflow.
    let wf_full = format!(
        "projects/{}/locations/{}/workflows/{}",
        TEST_PROJECT, TEST_REGION, wf_name
    );
    let body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", TEST_PROJECT, TEST_REGION, sched_name),
        "schedule": "0 0 1 1 *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": format!("https://workflowexecutions.googleapis.com/v1/{}/executions", wf_full),
            "httpMethod": "POST",
            "oauthToken": {
                "serviceAccountEmail": sa(),
                "scope": "https://www.googleapis.com/auth/cloud-platform",
            },
        },
    });
    create_scheduler_job_with_retry(&client, &body).await;

    // 3. Verify scheduler.
    let sched = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    assert!(sched.state == "ENABLED" || sched.state == "ACTIVE");

    // 4. Patch schedule.
    let patch = serde_json::json!({
        "schedule": "0 3 * * *",
        "timeZone": "America/Toronto",
        "httpTarget": {
            "uri": format!("https://workflowexecutions.googleapis.com/v1/{}/executions", wf_full),
            "httpMethod": "POST",
            "oauthToken": {
                "serviceAccountEmail": sa(),
                "scope": "https://www.googleapis.com/auth/cloud-platform",
            },
        },
    });
    let patched = client
        .patch_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name, &patch)
        .await
        .unwrap();
    assert_eq!(patched.schedule, "0 3 * * *");

    // 5. Cleanup.
    client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_gcp_dataproc_export_workflow_create_and_delete() {
    use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

    let client = gcp_client().await;
    let wf_name = unique_name("dp_export");

    let yaml = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        "test-export",
        "gcr.io/proj/spark:latest",
        "gs://bucket/export.py",
        &["gs://jars/jtds.jar"],
        &[
            "process",
            "analytics",
            "daily_report",
            "jdbc:sqlserver://host:1433",
            "secret",
            "dest.dbo.t",
        ],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        true,
    );

    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
    assert_eq!(wf.state, "ACTIVE");

    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();

    for _ in 0..15 {
        match client
            .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
            .await
        {
            Err(_) => return,
            Ok(wf) if wf.state != "ACTIVE" => return,
            _ => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
        }
    }
    let result = client
        .get_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await;
    assert!(result.is_err() || result.unwrap().state != "ACTIVE");
}

#[tokio::test]
async fn test_gcp_dataproc_export_scheduler_full_lifecycle() {
    use pulumi_resource_gcpx::dataproc::workflow_template::generate_dataproc_workflow_yaml;

    let client = gcp_client().await;
    ensure_scheduler_api().await;
    let base_name = unique_name("dp_exp_lc");
    let wf_name = format!("gcpx-wf-export-{}", base_name);
    let sched_name = format!("gcpx-sched-export-{}", base_name);

    let yaml = generate_dataproc_workflow_yaml(
        TEST_PROJECT,
        TEST_REGION,
        &base_name,
        "gcr.io/proj/spark:latest",
        "gs://bucket/export.py",
        &["gs://jars/jtds.jar"],
        &[
            "process",
            "ds",
            "tbl",
            "jdbc:sqlserver://host",
            "secret",
            "dest.dbo.t",
        ],
        "2.2",
        &sa(),
        "staging-bucket",
        &format!("projects/{TEST_PROJECT}/regions/{TEST_REGION}/subnetworks/default"),
        &["spark"],
        true,
    );
    client
        .create_workflow(TEST_PROJECT, TEST_REGION, &wf_name, &yaml, &sa())
        .await
        .unwrap();
    wait_workflow_active(&client, &wf_name).await;

    let wf_full = format!(
        "projects/{}/locations/{}/workflows/{}",
        TEST_PROJECT, TEST_REGION, wf_name
    );
    let body = serde_json::json!({
        "name": format!("projects/{}/locations/{}/jobs/{}", TEST_PROJECT, TEST_REGION, sched_name),
        "schedule": "0 0 1 1 *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": format!("https://workflowexecutions.googleapis.com/v1/{}/executions", wf_full),
            "httpMethod": "POST",
            "oauthToken": {
                "serviceAccountEmail": sa(),
                "scope": "https://www.googleapis.com/auth/cloud-platform",
            },
        },
    });
    create_scheduler_job_with_retry(&client, &body).await;

    let sched = client
        .get_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    assert!(sched.state == "ENABLED" || sched.state == "ACTIVE");

    // Cleanup.
    client
        .delete_scheduler_job(TEST_PROJECT, TEST_REGION, &sched_name)
        .await
        .unwrap();
    client
        .delete_workflow(TEST_PROJECT, TEST_REGION, &wf_name)
        .await
        .unwrap();
}
