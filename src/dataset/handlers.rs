use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::{BqOps, DatasetMeta};
use crate::dataset::api_body::{build_create_body, build_patch_body};
use crate::dataset::diff::compute_dataset_diff;
use crate::dataset::parse::{build_dataset_output, parse_dataset_inputs};
use crate::dataset::validate::validate_dataset;
use crate::error::IntoStatus;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::prost_util::parse_resource_id_2;
use crate::provider::GcpxProvider;

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_dataset(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_dataset(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_dataset(
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

        let old_inputs = parse_dataset_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;

        let diff = compute_dataset_diff(&old_inputs, &new_inputs);

        Ok(build_diff_response(&diff.replace_keys, &diff.update_keys))
    }

    pub async fn create_dataset(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_dataset_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!("{}/{}", inputs.project, inputs.dataset_id);

        let meta = if !req.preview {
            let body = build_create_body(&inputs);
            let all_keys: &[&str] = &[
                "description",
                "friendlyName",
                "labels",
                "defaultTableExpirationMs",
                "defaultPartitionExpirationMs",
                "storageBillingModel",
                "maxTimeTravelHours",
            ];
            let patch = build_patch_body(&inputs, all_keys);
            create_or_adopt(
                self.client.create_dataset(inputs.project, &body),
                || {
                    self.client
                        .patch_dataset(inputs.project, inputs.dataset_id, &patch)
                },
                "dataset",
            )
            .await?
        } else {
            DatasetMeta::preview(
                inputs.dataset_id,
                inputs.location,
                inputs.storage_billing_model,
            )
        };

        let outputs = build_dataset_output(&inputs, &meta);

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_dataset(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let (proj, ds_id) = parse_resource_id_2(&req.id).map_err(Status::invalid_argument)?;

        let meta = self
            .client
            .get_dataset(proj, ds_id)
            .await
            .status_internal()?;

        let inputs = crate::dataset::types::DatasetInputs {
            project: proj,
            dataset_id: ds_id,
            location: &meta.location,
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
            default_table_expiration_ms: meta.default_table_expiration_ms,
            default_partition_expiration_ms: meta.default_partition_expiration_ms,
            storage_billing_model: if meta.storage_billing_model.is_empty() {
                None
            } else {
                Some(&meta.storage_billing_model)
            },
            max_time_travel_hours: meta.max_time_travel_hours,
        };

        let outputs = build_dataset_output(&inputs, &meta);
        let inputs_out = outputs.clone();

        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: Some(outputs),
            inputs: Some(inputs_out),
            ..Default::default()
        }))
    }

    pub async fn update_dataset(
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

        let old_inputs = parse_dataset_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;

        let diff = compute_dataset_diff(&old_inputs, &new_inputs);

        let meta = if !req.preview && !diff.update_keys.is_empty() {
            let body = build_patch_body(&new_inputs, &diff.update_keys);
            self.client
                .patch_dataset(new_inputs.project, new_inputs.dataset_id, &body)
                .await
                .status_internal()?
        } else {
            DatasetMeta::preview(
                new_inputs.dataset_id,
                new_inputs.location,
                new_inputs.storage_billing_model,
            )
        };

        let outputs = build_dataset_output(&new_inputs, &meta);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_dataset(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        let (proj, ds_id) = parse_resource_id_2(&req.id).map_err(Status::invalid_argument)?;

        crate::lifecycle::verified_delete(
            self.client.delete_dataset(proj, ds_id),
            || self.client.get_dataset(proj, ds_id),
            10,
            std::time::Duration::from_secs(1),
        )
        .await
    }
}
