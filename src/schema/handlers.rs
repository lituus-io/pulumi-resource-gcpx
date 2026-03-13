use std::collections::HashMap;

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::BqOps;
use crate::error::IntoStatus;
use crate::handler_util::build_check_response;
use crate::prost_util::get_struct_fields;
use crate::provider::GcpxProvider;
use crate::schema::ddl::build_batch_ddl;
use crate::schema::diff::compute_diff;
use crate::schema::parse::{build_output_from_bq_fields, build_schema_output, parse_schema_inputs};
use crate::schema::types::{clean_fields, field_to_json, AlterAction, DdlOp};
use crate::schema::validate::{validate, validate_schema_drop_safety, ConsumerConstraints};

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_table_schema(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let (_proj, _ds, _tid, new_fields) =
            parse_schema_inputs(news).map_err(Status::invalid_argument)?;

        // Pulumi engine may send olds as Some(empty struct) for new resources.
        // Treat empty or unparseable olds as a create (no old state).
        let old_parsed;
        let (is_create, old_fields_ref) = if let Some(olds) = req.olds.as_ref() {
            if olds.fields.is_empty() {
                (true, None)
            } else {
                match parse_schema_inputs(olds) {
                    Ok(parsed) => {
                        old_parsed = parsed;
                        (false, Some(old_parsed.3.as_slice()))
                    }
                    // Olds may lack expected fields during engine-initiated checks.
                    Err(_) => (true, None),
                }
            }
        } else {
            (true, None)
        };

        let mut failures = validate(&new_fields, is_create, old_fields_ref);

        // Drop safety: validate that dropped columns don't conflict with consumer constraints.
        if failures.is_empty() {
            if let Some(constraints) = parse_consumer_constraints(news) {
                let drops: Vec<&str> = new_fields
                    .iter()
                    .filter(|f| f.alter == Some(AlterAction::Delete))
                    .map(|f| f.name)
                    .collect();
                if !drops.is_empty() {
                    failures.extend(validate_schema_drop_safety(&drops, &constraints));
                }
            }
        }

        build_check_response(req.news, failures)
    }

    pub async fn diff_table_schema(
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

        // Engine may send empty olds during initial preview — treat as no old state.
        if olds.fields.is_empty() {
            return Ok(Response::new(pulumirpc::DiffResponse {
                changes: pulumirpc::diff_response::DiffChanges::DiffSome as i32,
                ..Default::default()
            }));
        }

        let (old_proj, old_ds, old_tid, old_fields) =
            parse_schema_inputs(olds).map_err(Status::internal)?;
        let (new_proj, new_ds, new_tid, new_fields) =
            parse_schema_inputs(news).map_err(Status::invalid_argument)?;

        let diff_result = compute_diff(
            old_proj,
            new_proj,
            old_ds,
            new_ds,
            old_tid,
            new_tid,
            &old_fields,
            &new_fields,
        );

        let changes = if diff_result.has_schema_changes || !diff_result.replace_keys.is_empty() {
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        } else {
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        };

        let replaces: Vec<String> = diff_result
            .replace_keys
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut detailed_diff = HashMap::new();
        if diff_result.has_schema_changes {
            detailed_diff.insert(
                "schema".to_owned(),
                pulumirpc::PropertyDiff {
                    kind: pulumirpc::property_diff::Kind::Update as i32,
                    input_diff: true,
                },
            );
        }
        for key in &diff_result.replace_keys {
            detailed_diff.insert(
                key.to_string(),
                pulumirpc::PropertyDiff {
                    kind: pulumirpc::property_diff::Kind::UpdateReplace as i32,
                    input_diff: true,
                },
            );
        }

        Ok(Response::new(pulumirpc::DiffResponse {
            changes,
            replaces,
            has_detailed_diff: true,
            detailed_diff,
            ..Default::default()
        }))
    }

    pub async fn create_table_schema(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let (proj, ds, tid, fields) =
            parse_schema_inputs(props).map_err(Status::invalid_argument)?;

        let clean = clean_fields(&fields);

        // Apply schema to BigQuery table via patch API.
        if !req.preview && !clean.is_empty() {
            let bq_fields: Vec<serde_json::Value> = clean.iter().map(field_to_json).collect();
            let body = serde_json::json!({
                "schema": { "fields": bq_fields }
            });
            self.client
                .patch_table(proj, ds, tid, &body)
                .await
                .status_internal()?;
        }

        let id = format!("{}/{}/{}", proj, ds, tid);
        let outputs = build_schema_output(proj, ds, tid, &clean, &[]);

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn read_table_schema(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let (proj, ds, tid) =
            crate::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

        let bq_fields = self
            .client
            .get_table_schema(proj, ds, tid)
            .await
            .status_internal()?;

        let outputs = build_output_from_bq_fields(proj, ds, tid, &bq_fields);
        // ReadResponse needs both properties and inputs -- clone is unavoidable here.
        let inputs = outputs.clone();

        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: Some(outputs),
            inputs: Some(inputs),
            ..Default::default()
        }))
    }

    pub async fn update_table_schema(
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

        let (old_proj, old_ds, old_tid, old_fields) =
            parse_schema_inputs(olds).map_err(Status::internal)?;
        let (new_proj, new_ds, new_tid, new_fields) =
            parse_schema_inputs(news).map_err(Status::invalid_argument)?;

        let diff_result = compute_diff(
            old_proj,
            new_proj,
            old_ds,
            new_ds,
            old_tid,
            new_tid,
            &old_fields,
            &new_fields,
        );

        // Drop safety: validate before executing any DDL.
        if let Some(constraints) = parse_consumer_constraints(news) {
            let drops: Vec<&str> = diff_result
                .ops
                .iter()
                .filter_map(|op| match op {
                    DdlOp::DropColumn { name } => Some(*name),
                    _ => None,
                })
                .collect();
            if !drops.is_empty() {
                let failures = validate_schema_drop_safety(&drops, &constraints);
                if !failures.is_empty() {
                    let msg = failures
                        .iter()
                        .map(|f| format!("  - {}", f.reason))
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(Status::failed_precondition(format!(
                        "Schema drop safety check failed:\n{}",
                        msg
                    )));
                }
            }
        }

        let ddl_stmts = build_batch_ddl(
            new_proj,
            new_ds,
            new_tid,
            &diff_result.ops,
            &diff_result.owned_ops,
        );

        if !req.preview && !ddl_stmts.is_empty() {
            if ddl_stmts.len() == 1 {
                self.client
                    .execute_ddl(new_proj, &ddl_stmts[0], None)
                    .await
                    .status_internal()?;
            } else {
                // Wrap multiple DDL statements in a BigQuery scripting block
                // to execute as a single request instead of sequential calls
                // with 2s sleep between each.
                let mut script = String::from("BEGIN\n");
                for stmt in &ddl_stmts {
                    script.push_str("  ");
                    script.push_str(stmt);
                    script.push_str(";\n");
                }
                script.push_str("END;");
                self.client
                    .execute_ddl(new_proj, &script, None)
                    .await
                    .status_internal()?;
            }
        }

        let clean = clean_fields(&new_fields);
        let pending = if req.preview { &ddl_stmts } else { &vec![] };
        let outputs = build_schema_output(new_proj, new_ds, new_tid, &clean, pending);

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(outputs),
            ..Default::default()
        }))
    }

    pub async fn delete_table_schema(
        &self,
        _req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        // No-op: gcp:bigquery:Table owns the table lifecycle.
        Ok(Response::new(()))
    }
}

/// Parse `consumerConstraints` from properties, if present.
fn parse_consumer_constraints(props: &prost_types::Struct) -> Option<ConsumerConstraints<'_>> {
    let cc = get_struct_fields(&props.fields, "consumerConstraints")?;

    let extract_str_list = |key: &str| -> Vec<&str> {
        crate::prost_util::get_list(cc, key)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| match &v.kind {
                        Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Some(ConsumerConstraints {
        unique_keys: extract_str_list("uniqueKeys"),
        partition_fields: extract_str_list("partitionFields"),
        cluster_columns: extract_str_list("clusterColumns"),
    })
}
