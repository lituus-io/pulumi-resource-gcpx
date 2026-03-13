use pulumi_rs_yaml_proto::pulumirpc;
use pulumi_rs_yaml_proto::pulumirpc::resource_provider_server::ResourceProvider;
use tonic::{Request, Response, Status};

use crate::bq::BqOps;
use crate::prost_util::resource_type_from_urn;
use crate::scheduler_ops::SchedulerOps;

const SCHEMA_JSON: &str = include_str!("../schema.json");

const TABLE_SCHEMA: &str = "gcpx:bigquery/tableSchema:TableSchema";
const TABLE: &str = "gcpx:bigquery/table:Table";
const DATASET: &str = "gcpx:bigquery/dataset:Dataset";
const ROUTINE: &str = "gcpx:bigquery/routineFunction:RoutineFunction";
const DBT_PROJECT: &str = "gcpx:dbt/project:Project";
const DBT_MODEL: &str = "gcpx:dbt/model:Model";
const DBT_MACRO: &str = "gcpx:dbt/macro:Macro";
const SQL_JOB: &str = "gcpx:scheduler/sqlJob:SqlJob";
const DBT_SNAPSHOT: &str = "gcpx:dbt/snapshot:Snapshot";
const INGEST_JOB: &str = "gcpx:dataproc/ingestJob:IngestJob";
const EXPORT_JOB: &str = "gcpx:dataproc/exportJob:ExportJob";

macro_rules! dispatch {
    ($self:expr, $req:expr, $( $type_const:ident => $handler:ident ),+ $(,)?) => {
        match resource_type_from_urn(&$req.urn) {
            $( $type_const => $self.$handler($req).await, )+
            other => Err(Status::not_found(format!("unknown resource type: {other}"))),
        }
    }
}

pub struct GcpxProvider<C> {
    pub client: C,
}

impl<C> GcpxProvider<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl<C: BqOps + SchedulerOps> ResourceProvider for GcpxProvider<C> {
    async fn handshake(
        &self,
        _request: Request<pulumirpc::ProviderHandshakeRequest>,
    ) -> Result<Response<pulumirpc::ProviderHandshakeResponse>, Status> {
        Ok(Response::new(pulumirpc::ProviderHandshakeResponse {
            accept_secrets: true,
            accept_resources: true,
            accept_outputs: false,
            supports_autonaming_configuration: false,
        }))
    }

    async fn parameterize(
        &self,
        _request: Request<pulumirpc::ParameterizeRequest>,
    ) -> Result<Response<pulumirpc::ParameterizeResponse>, Status> {
        Err(Status::unimplemented("parameterize is not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<pulumirpc::GetSchemaRequest>,
    ) -> Result<Response<pulumirpc::GetSchemaResponse>, Status> {
        Ok(Response::new(pulumirpc::GetSchemaResponse {
            schema: SCHEMA_JSON.to_owned(),
        }))
    }

    async fn check_config(
        &self,
        request: Request<pulumirpc::CheckRequest>,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(pulumirpc::CheckResponse {
            inputs: req.news,
            failures: vec![],
        }))
    }

    async fn diff_config(
        &self,
        _request: Request<pulumirpc::DiffRequest>,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        Ok(Response::new(pulumirpc::DiffResponse::default()))
    }

    async fn configure(
        &self,
        _request: Request<pulumirpc::ConfigureRequest>,
    ) -> Result<Response<pulumirpc::ConfigureResponse>, Status> {
        Ok(Response::new(pulumirpc::ConfigureResponse {
            accept_secrets: true,
            supports_preview: true,
            accept_resources: true,
            accept_outputs: false,
            supports_autonaming_configuration: false,
        }))
    }

    async fn invoke(
        &self,
        _request: Request<pulumirpc::InvokeRequest>,
    ) -> Result<Response<pulumirpc::InvokeResponse>, Status> {
        Err(Status::unimplemented("invoke is not supported"))
    }

    async fn call(
        &self,
        _request: Request<pulumirpc::CallRequest>,
    ) -> Result<Response<pulumirpc::CallResponse>, Status> {
        Err(Status::unimplemented("call is not supported"))
    }

    async fn check(
        &self,
        request: Request<pulumirpc::CheckRequest>,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => check_table_schema,
            TABLE => check_table,
            DATASET => check_dataset,
            ROUTINE => check_routine,
            DBT_PROJECT => check_dbt_project,
            DBT_MODEL => check_dbt_model,
            DBT_MACRO => check_dbt_macro,
            SQL_JOB => check_sql_job,
            DBT_SNAPSHOT => check_snapshot,
            INGEST_JOB => check_ingest_job,
            EXPORT_JOB => check_export_job,
        )
    }

    async fn diff(
        &self,
        request: Request<pulumirpc::DiffRequest>,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => diff_table_schema,
            TABLE => diff_table,
            DATASET => diff_dataset,
            ROUTINE => diff_routine,
            DBT_PROJECT => diff_dbt_project,
            DBT_MODEL => diff_dbt_model,
            DBT_MACRO => diff_dbt_macro,
            SQL_JOB => diff_sql_job,
            DBT_SNAPSHOT => diff_snapshot,
            INGEST_JOB => diff_ingest_job,
            EXPORT_JOB => diff_export_job,
        )
    }

    async fn create(
        &self,
        request: Request<pulumirpc::CreateRequest>,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => create_table_schema,
            TABLE => create_table,
            DATASET => create_dataset,
            ROUTINE => create_routine,
            DBT_PROJECT => create_dbt_project,
            DBT_MODEL => create_dbt_model,
            DBT_MACRO => create_dbt_macro,
            SQL_JOB => create_sql_job,
            DBT_SNAPSHOT => create_snapshot,
            INGEST_JOB => create_ingest_job,
            EXPORT_JOB => create_export_job,
        )
    }

    async fn read(
        &self,
        request: Request<pulumirpc::ReadRequest>,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => read_table_schema,
            TABLE => read_table,
            DATASET => read_dataset,
            ROUTINE => read_routine,
            DBT_PROJECT => read_dbt_project,
            DBT_MODEL => read_dbt_model,
            DBT_MACRO => read_dbt_macro,
            SQL_JOB => read_sql_job,
            DBT_SNAPSHOT => read_snapshot,
            INGEST_JOB => read_ingest_job,
            EXPORT_JOB => read_export_job,
        )
    }

    async fn update(
        &self,
        request: Request<pulumirpc::UpdateRequest>,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => update_table_schema,
            TABLE => update_table,
            DATASET => update_dataset,
            ROUTINE => update_routine,
            DBT_PROJECT => update_dbt_project,
            DBT_MODEL => update_dbt_model,
            DBT_MACRO => update_dbt_macro,
            SQL_JOB => update_sql_job,
            DBT_SNAPSHOT => update_snapshot,
            INGEST_JOB => update_ingest_job,
            EXPORT_JOB => update_export_job,
        )
    }

    async fn delete(
        &self,
        request: Request<pulumirpc::DeleteRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => delete_table_schema,
            TABLE => delete_table,
            DATASET => delete_dataset,
            ROUTINE => delete_routine,
            DBT_PROJECT => delete_dbt_project,
            DBT_MODEL => delete_dbt_model,
            DBT_MACRO => delete_dbt_macro,
            SQL_JOB => delete_sql_job,
            DBT_SNAPSHOT => delete_snapshot,
            INGEST_JOB => delete_ingest_job,
            EXPORT_JOB => delete_export_job,
        )
    }

    async fn construct(
        &self,
        _request: Request<pulumirpc::ConstructRequest>,
    ) -> Result<Response<pulumirpc::ConstructResponse>, Status> {
        Err(Status::unimplemented("construct is not supported"))
    }

    async fn cancel(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_plugin_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<pulumirpc::PluginInfo>, Status> {
        Ok(Response::new(pulumirpc::PluginInfo {
            version: "0.1.0".to_owned(),
        }))
    }

    async fn attach(
        &self,
        _request: Request<pulumirpc::PluginAttach>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_mapping(
        &self,
        _request: Request<pulumirpc::GetMappingRequest>,
    ) -> Result<Response<pulumirpc::GetMappingResponse>, Status> {
        Ok(Response::new(pulumirpc::GetMappingResponse::default()))
    }

    async fn get_mappings(
        &self,
        _request: Request<pulumirpc::GetMappingsRequest>,
    ) -> Result<Response<pulumirpc::GetMappingsResponse>, Status> {
        Ok(Response::new(pulumirpc::GetMappingsResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bq::BqField;
    use crate::gcp_client::MockGcpClient;
    use crate::prost_util::{prost_list, prost_string};

    fn prost_struct_val(fields: Vec<(&str, prost_types::Value)>) -> prost_types::Value {
        prost_types::Value {
            kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                fields: fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
            })),
        }
    }

    fn make_field(name: &str, ty: &str) -> prost_types::Value {
        prost_struct_val(vec![
            ("name", prost_string(name)),
            ("type", prost_string(ty)),
            ("mode", prost_string("NULLABLE")),
        ])
    }

    fn make_schema_inputs(
        project: &str,
        dataset: &str,
        table_id: &str,
        fields: Vec<prost_types::Value>,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string(project)),
                ("dataset".to_owned(), prost_string(dataset)),
                ("tableId".to_owned(), prost_string(table_id)),
                ("schema".to_owned(), prost_list(fields)),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn mock_provider() -> GcpxProvider<MockGcpClient> {
        GcpxProvider::new(MockGcpClient::new(vec![]))
    }

    #[tokio::test]
    async fn get_schema_returns_json() {
        let p = mock_provider();
        let resp = p
            .get_schema(Request::new(pulumirpc::GetSchemaRequest::default()))
            .await
            .unwrap();
        let schema = resp.into_inner();
        assert!(schema
            .schema
            .contains("gcpx:bigquery/tableSchema:TableSchema"));
        assert!(schema.schema.contains("gcpx:bigquery/table:Table"));
        assert!(schema.schema.contains("gcpx:bigquery/dataset:Dataset"));
        assert!(schema
            .schema
            .contains("gcpx:bigquery/routineFunction:RoutineFunction"));
        assert!(schema.schema.contains("gcpx:dbt/project:Project"));
        assert!(schema.schema.contains("gcpx:dbt/model:Model"));
        assert!(schema.schema.contains("gcpx:dbt/macro:Macro"));
        assert!(schema.schema.contains("gcpx:scheduler/sqlJob:SqlJob"));
        assert!(schema.schema.contains("gcpx:dbt/snapshot:Snapshot"));
    }

    #[tokio::test]
    async fn check_routes_table_schema() {
        let p = mock_provider();
        let news = make_schema_inputs("proj", "ds", "tbl", vec![make_field("col_a", "STRING")]);

        let resp = p
            .check(Request::new(pulumirpc::CheckRequest {
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
                olds: None,
                news: Some(news),
                ..Default::default()
            }))
            .await
            .unwrap();

        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_unknown_resource_type() {
        let p = mock_provider();
        let news = prost_types::Struct::default();

        let result = p
            .check(Request::new(pulumirpc::CheckRequest {
                urn: "urn:pulumi:stack::project::gcpx:unknown:Foo::test".into(),
                olds: None,
                news: Some(news),
                ..Default::default()
            }))
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn create_routes_table_schema() {
        let p = mock_provider();
        let props = make_schema_inputs("proj", "ds", "tbl", vec![make_field("col_a", "STRING")]);

        let resp = p
            .create(Request::new(pulumirpc::CreateRequest {
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
                properties: Some(props),
                ..Default::default()
            }))
            .await
            .unwrap();

        let inner = resp.into_inner();
        assert_eq!(inner.id, "proj/ds/tbl");
        assert!(inner.properties.is_some());
    }

    #[tokio::test]
    async fn delete_table_schema_is_noop() {
        let p = mock_provider();
        let resp = p
            .delete(Request::new(pulumirpc::DeleteRequest {
                id: "proj/ds/tbl".into(),
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
                ..Default::default()
            }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn read_routes_table_schema() {
        let mock = MockGcpClient::new(vec![BqField {
            name: "col_a".into(),
            field_type: "STRING".into(),
            mode: "NULLABLE".into(),
            description: "test column".into(),
            fields: vec![],
        }]);
        let p = GcpxProvider::new(mock);

        let resp = p
            .read(Request::new(pulumirpc::ReadRequest {
                id: "proj/ds/tbl".into(),
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
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
    async fn diff_routes_table_schema() {
        let p = mock_provider();
        let schema = vec![make_field("col_a", "STRING")];
        let olds = make_schema_inputs("proj", "ds", "tbl", schema.clone());
        let news = make_schema_inputs("proj", "ds", "tbl", schema);

        let resp = p
            .diff(Request::new(pulumirpc::DiffRequest {
                id: "proj/ds/tbl".into(),
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            }))
            .await
            .unwrap();

        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        );
    }

    #[tokio::test]
    async fn update_routes_table_schema() {
        let mock = MockGcpClient::new(vec![]);
        let p = GcpxProvider::new(mock);

        let olds = make_schema_inputs("proj", "ds", "tbl", vec![make_field("col_a", "STRING")]);

        let insert_field = prost_struct_val(vec![
            ("name", prost_string("col_b")),
            ("type", prost_string("INT64")),
            ("mode", prost_string("NULLABLE")),
            ("alter", prost_string("insert")),
        ]);
        let news = make_schema_inputs(
            "proj",
            "ds",
            "tbl",
            vec![make_field("col_a", "STRING"), insert_field],
        );

        let resp = p
            .update(Request::new(pulumirpc::UpdateRequest {
                id: "proj/ds/tbl".into(),
                urn: "urn:pulumi:stack::project::gcpx:bigquery/tableSchema:TableSchema::test"
                    .into(),
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            }))
            .await
            .unwrap();

        assert!(resp.into_inner().properties.is_some());

        let ddl_log = p.client.ddl_log();
        assert!(!ddl_log.is_empty());
        assert!(ddl_log[0].contains("ADD COLUMN"));
    }
}
