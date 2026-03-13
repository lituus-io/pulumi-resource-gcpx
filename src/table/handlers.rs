use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::{BqOps, BqTableMeta};
use crate::error::IntoStatus;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::provider::GcpxProvider;
use crate::table::api_body::{build_create_body, build_patch_body};
use crate::table::diff::compute_table_diff;
use crate::table::parse::{build_table_output, parse_table_inputs};
use crate::table::types::TableInputs;
use crate::table::validate::validate_table;

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_table(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_table_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_table(&inputs);
        build_check_response(req.news, failures)
    }

    pub async fn diff_table(
        &self,
        req: pulumirpc::DiffRequest,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let olds = req
            .olds
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing olds"))?;
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let old_inputs = parse_table_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_table_inputs(news).map_err(Status::invalid_argument)?;
        let diff = compute_table_diff(&old_inputs, &new_inputs);

        Ok(build_diff_response(&diff.replace_keys, &diff.update_keys))
    }

    pub async fn create_table(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_table_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!("{}/{}/{}", inputs.project, inputs.dataset, inputs.table_id);

        let meta = if !req.preview {
            let body = build_create_body(&inputs);
            let all_keys: &[&str] = &[
                "description",
                "friendlyName",
                "labels",
                "expirationTime",
                "deletionProtection",
                "clusterings",
                "storageBillingModel",
            ];
            let patch = build_patch_body(&inputs, all_keys);
            create_or_adopt(
                self.client
                    .create_table(inputs.project, inputs.dataset, &body),
                || {
                    self.client
                        .patch_table(inputs.project, inputs.dataset, inputs.table_id, &patch)
                },
                "table",
            )
            .await?
        } else {
            let table_type = if inputs.view.is_some() {
                "VIEW"
            } else if inputs.materialized_view.is_some() {
                "MATERIALIZED_VIEW"
            } else if inputs.external_data_config.is_some() {
                "EXTERNAL"
            } else {
                "TABLE"
            };
            BqTableMeta::preview(table_type)
        };

        let outputs =
            build_table_output(&inputs, &meta.table_type, meta.num_rows, meta.creation_time);

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_table(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let (proj, ds, tid) =
            crate::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

        let meta = self
            .client
            .get_table(proj, ds, tid)
            .await
            .status_internal()?;

        let inputs_struct = TableInputs {
            project: proj,
            dataset: ds,
            table_id: tid,
            description: if meta.description.is_empty() {
                None
            } else {
                Some(&meta.description)
            },
            friendly_name: if meta.friendly_name.is_empty() {
                None
            } else {
                Some(&meta.friendly_name)
            },
            labels: meta
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            view: None,
            materialized_view: None,
            time_partitioning: None,
            range_partitioning: None,
            clusterings: vec![],
            deletion_protection: None,
            expiration_time: meta.expiration_time,
            encryption_kms_key: None,
            storage_billing_model: None,
            max_staleness: None,
            external_data_config: None,
        };

        let outputs = build_table_output(
            &inputs_struct,
            &meta.table_type,
            meta.num_rows,
            meta.creation_time,
        );
        let inputs_out = outputs.clone();

        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: Some(outputs),
            inputs: Some(inputs_out),
            ..Default::default()
        }))
    }

    pub async fn update_table(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let olds = req
            .olds
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing olds"))?;
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let old_inputs = parse_table_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_table_inputs(news).map_err(Status::invalid_argument)?;

        let diff = compute_table_diff(&old_inputs, &new_inputs);

        if !req.preview && !diff.update_keys.is_empty() {
            let body = build_patch_body(&new_inputs, &diff.update_keys);
            self.client
                .patch_table(
                    new_inputs.project,
                    new_inputs.dataset,
                    new_inputs.table_id,
                    &body,
                )
                .await
                .status_internal()?;
        }

        let table_type = if new_inputs.view.is_some() {
            "VIEW"
        } else if new_inputs.materialized_view.is_some() {
            "MATERIALIZED_VIEW"
        } else {
            "TABLE"
        };

        let outputs = build_table_output(&new_inputs, table_type, 0, 0);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_table(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        let (proj, ds, tid) =
            crate::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

        crate::lifecycle::verified_delete(
            self.client.delete_table(proj, ds, tid),
            || self.client.get_table(proj, ds, tid),
            10,
            std::time::Duration::from_secs(1),
        )
        .await
    }
}
