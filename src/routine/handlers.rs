use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::{BqOps, RoutineMeta};
use crate::error::IntoStatus;
use crate::handler_util::{build_check_response, build_diff_response};
use crate::lifecycle::create_or_adopt;
use crate::provider::GcpxProvider;
use crate::routine::api_body::{build_create_body, build_update_body};
use crate::routine::diff::compute_routine_diff;
use crate::routine::parse::{build_routine_output, parse_routine_inputs};
use crate::routine::validate::validate_routine;

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_routine(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_routine(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_routine(
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

        let old_inputs = parse_routine_inputs(olds).map_err(Status::internal)?;
        let new_inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;

        let diff = compute_routine_diff(&old_inputs, &new_inputs);

        Ok(build_diff_response(&diff.replace_keys, &diff.update_keys))
    }

    pub async fn create_routine(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_routine_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!(
            "{}/{}/{}",
            inputs.project, inputs.dataset, inputs.routine_id
        );

        let meta = if !req.preview {
            let body = build_create_body(&inputs);
            let update_body = build_update_body(&inputs);
            create_or_adopt(
                self.client
                    .create_routine(inputs.project, inputs.dataset, &body),
                || {
                    self.client.update_routine(
                        inputs.project,
                        inputs.dataset,
                        inputs.routine_id,
                        &update_body,
                    )
                },
                "routine",
            )
            .await?
        } else {
            RoutineMeta::preview(inputs.routine_id, inputs.routine_type, inputs.language)
        };

        let outputs = build_routine_output(&inputs, &meta);

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_routine(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let (proj, ds, rid) =
            crate::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

        let meta = self
            .client
            .get_routine(proj, ds, rid)
            .await
            .status_internal()?;

        // Build minimal inputs from read metadata.
        let inputs = crate::routine::types::RoutineInputs {
            project: proj,
            dataset: ds,
            routine_id: rid,
            routine_type: &meta.routine_type,
            language: &meta.language,
            definition_body: "",
            description: None,
            arguments: vec![],
            return_type: None,
            imported_libraries: vec![],
            determinism_level: None,
        };

        let outputs = build_routine_output(&inputs, &meta);
        let inputs_out = outputs.clone();

        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: Some(outputs),
            inputs: Some(inputs_out),
            ..Default::default()
        }))
    }

    pub async fn update_routine(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let new_inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;

        let meta = if !req.preview {
            let body = build_update_body(&new_inputs);
            self.client
                .update_routine(
                    new_inputs.project,
                    new_inputs.dataset,
                    new_inputs.routine_id,
                    &body,
                )
                .await
                .status_internal()?
        } else {
            RoutineMeta::preview(
                new_inputs.routine_id,
                new_inputs.routine_type,
                new_inputs.language,
            )
        };

        let outputs = build_routine_output(&new_inputs, &meta);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_routine(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        let (proj, ds, rid) =
            crate::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

        crate::lifecycle::verified_delete(
            self.client.delete_routine(proj, ds, rid),
            || self.client.get_routine(proj, ds, rid),
            10,
            std::time::Duration::from_secs(1),
        )
        .await
    }
}
