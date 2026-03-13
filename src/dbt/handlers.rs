use std::collections::{BTreeMap, HashMap};

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::bq::BqOps;
use crate::dbt::options::OwnedTableOptions;
use crate::dbt::parse::{
    build_macro_output, build_model_output_struct, build_project_context_output, parse_macro_defs,
    parse_macro_inputs, parse_model_refs, parse_project_context, parse_project_inputs,
};
use crate::dbt::preprocess::preprocess;
use crate::dbt::resolver::{
    generate_append_ddl, generate_ddl, generate_delete_insert_ddl, generate_merge_ddl,
    generate_merge_ddl_placeholder, generate_merge_replace_ddl, resolve,
};
use crate::dbt::scanner::{ConfigArgIter, ConfigValue, DbtScanner, DbtSegment};
use crate::dbt::types::{
    format_cost_estimate, MacroOutput, ModelConfig, ModelOutput, PartitionConfig,
};
use crate::dbt::validate::{
    diff_options, validate_macro, validate_max_bytes_billed, validate_model, validate_model_schema,
    validate_project,
};
use crate::error::IntoStatus;
use crate::handler_util::build_check_response;
use crate::prost_util::{
    get_bool, get_number, get_str, get_struct_fields, prost_string, prost_struct,
};
use crate::provider::GcpxProvider;
use crate::scheduler::workflow_template::generate_workflow_yaml;

// --- Project handlers ---

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_dbt_project(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_project_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_project(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_dbt_project(
        &self,
        req: pulumirpc::DiffRequest,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let input_keys: &[&str] = &[
            "gcpProject",
            "dataset",
            "sources",
            "declaredModels",
            "declaredMacros",
            "vars",
        ];
        let changes = if fields_equal(req.olds.as_ref(), req.news.as_ref(), input_keys) {
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        } else {
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        };

        Ok(Response::new(pulumirpc::DiffResponse {
            changes,
            ..Default::default()
        }))
    }

    pub async fn create_dbt_project(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_project_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!("dbt-project/{}/{}", inputs.gcp_project, inputs.dataset);

        let context_struct = build_project_context_output(&inputs);

        let mut output_fields = BTreeMap::new();
        output_fields.insert("gcpProject".to_owned(), prost_string(inputs.gcp_project));
        output_fields.insert("dataset".to_owned(), prost_string(inputs.dataset));
        output_fields.insert("context".to_owned(), prost_struct(context_struct.fields));

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn read_dbt_project(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: req.properties,
            inputs: req.inputs,
            ..Default::default()
        }))
    }

    pub async fn update_dbt_project(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_project_inputs(news).map_err(Status::invalid_argument)?;
        let context_struct = build_project_context_output(&inputs);

        let mut output_fields = BTreeMap::new();
        output_fields.insert("gcpProject".to_owned(), prost_string(inputs.gcp_project));
        output_fields.insert("dataset".to_owned(), prost_string(inputs.dataset));
        output_fields.insert("context".to_owned(), prost_struct(context_struct.fields));

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn delete_dbt_project(
        &self,
        _req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

// --- Macro handlers ---

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_dbt_macro(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_macro_inputs(news).map_err(Status::invalid_argument)?;
        let failures = validate_macro(&inputs);

        build_check_response(req.news, failures)
    }

    pub async fn diff_dbt_macro(
        &self,
        req: pulumirpc::DiffRequest,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let input_keys: &[&str] = &["name", "sql", "args"];
        let changes = if fields_equal(req.olds.as_ref(), req.news.as_ref(), input_keys) {
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        } else {
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        };

        Ok(Response::new(pulumirpc::DiffResponse {
            changes,
            ..Default::default()
        }))
    }

    pub async fn create_dbt_macro(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let inputs = parse_macro_inputs(props).map_err(Status::invalid_argument)?;
        let id = format!("dbt-macro/{}", inputs.name);

        let output = MacroOutput {
            name: inputs.name.to_owned(),
            args: inputs.args.iter().map(|a| a.to_string()).collect(),
            sql: inputs.sql.to_owned(),
        };

        let mut output_fields = BTreeMap::new();
        output_fields.insert("name".to_owned(), prost_string(inputs.name));
        output_fields.insert("sql".to_owned(), prost_string(inputs.sql));
        output_fields.insert(
            "macroOutput".to_owned(),
            prost_struct(build_macro_output(&output).fields),
        );

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn read_dbt_macro(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: req.properties,
            inputs: req.inputs,
            ..Default::default()
        }))
    }

    pub async fn update_dbt_macro(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let inputs = parse_macro_inputs(news).map_err(Status::invalid_argument)?;

        let output = MacroOutput {
            name: inputs.name.to_owned(),
            args: inputs.args.iter().map(|a| a.to_string()).collect(),
            sql: inputs.sql.to_owned(),
        };

        let mut output_fields = BTreeMap::new();
        output_fields.insert("name".to_owned(), prost_string(inputs.name));
        output_fields.insert("sql".to_owned(), prost_string(inputs.sql));
        output_fields.insert(
            "macroOutput".to_owned(),
            prost_struct(build_macro_output(&output).fields),
        );

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn delete_dbt_macro(
        &self,
        _req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

// --- Model handlers ---

impl<C: BqOps> GcpxProvider<C> {
    pub async fn check_dbt_model(
        &self,
        req: pulumirpc::CheckRequest,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let name = get_str(&news.fields, "name").unwrap_or("");
        let sql = get_str(&news.fields, "sql").unwrap_or("");
        let context = crate::prost_util::get_struct_fields(&news.fields, "context")
            .map(parse_project_context)
            .unwrap_or_else(|| crate::dbt::types::ProjectContext {
                gcp_project: "",
                dataset: "",
                sources: BTreeMap::new(),
                declared_models: vec![],
                declared_macros: vec![],
                vars: BTreeMap::new(),
            });
        let model_refs = parse_model_refs(&news.fields);
        let macros = parse_macro_defs(&news.fields);
        let max_bytes_billed = get_number(&news.fields, "maxBytesBilled").map(|n| n as i64);

        let mut config = extract_model_config(sql, &macros);
        let yaml_options = parse_resource_options(&news.fields);
        let table_id_input = get_str(&news.fields, "tableId");

        // Diff SQL config options vs YAML options (if YAML options declared)
        if let Some(ref yaml_opts) = yaml_options {
            let option_failures = diff_options(&config, yaml_opts);
            if !option_failures.is_empty() {
                return build_check_response(req.news, option_failures);
            }
            // YAML is authoritative — override config's option fields
            config.override_options(yaml_opts);
        }

        let mut failures = validate_model(name, sql, &config, &context, &model_refs, &macros);
        failures.extend(validate_max_bytes_billed(max_bytes_billed));

        // Mode 2 validation: options are ignored when tableId is set
        if table_id_input.is_some() && !config.to_table_options().is_empty() {
            failures.push(crate::schema::types::CheckFailure {
                property: "options".into(),
                reason: "Table options are ignored when 'tableId' is set (Mode 2). \
                         The table is managed by a separate Table/TableSchema resource. \
                         Remove options from the Model's SQL config block and/or YAML options property."
                    .into(),
            });
        }

        // E6: Dry-run SQL validation during check — only on create or when SQL changed.
        if failures.is_empty()
            && config.materialization != "ephemeral"
            && !sql.is_empty()
            && !context.gcp_project.is_empty()
        {
            let is_create = req.olds.as_ref().is_none_or(|o| o.fields.is_empty());
            let sql_changed = req
                .olds
                .as_ref()
                .and_then(|o| get_str(&o.fields, "sql"))
                .is_none_or(|old_sql| old_sql != sql);

            if is_create || sql_changed {
                let table_ref = format!("`{}.{}.{}`", context.gcp_project, context.dataset, name);
                if let Ok(preprocessed) = preprocess(sql, &context.vars, &table_ref, false) {
                    if let Ok(resolved) = resolve(
                        &preprocessed,
                        context.gcp_project,
                        context.dataset,
                        &context.sources,
                        &model_refs,
                        &macros,
                    ) {
                        let select_sql = resolved.to_sql();
                        if let Ok(dry_run) = self
                            .client
                            .dry_run_query(context.gcp_project, &select_sql, max_bytes_billed)
                            .await
                        {
                            if !dry_run.valid {
                                failures.push(crate::schema::types::CheckFailure {
                                    property: "sql".into(),
                                    reason: format!(
                                        "SQL dry-run validation failed: {}",
                                        dry_run.error_message.unwrap_or_default()
                                    )
                                    .into(),
                                });
                            }
                        }
                    }
                }
            }
        }

        build_check_response(req.news, failures)
    }

    pub async fn diff_dbt_model(
        &self,
        req: pulumirpc::DiffRequest,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        // Compare only stable input keys — exclude context/modelRefs which have
        // structural noise (computed fields, field ordering differences).
        let stable_keys: &[&str] = &[
            "name",
            "sql",
            "macros",
            "tableId",
            "schema",
            "options",
            "maxBytesBilled",
        ];
        let stable_equal = fields_equal(req.olds.as_ref(), req.news.as_ref(), stable_keys);
        let context_eq = context_fields_equal(req.olds.as_ref(), req.news.as_ref());
        let model_refs_eq = model_refs_equal(req.olds.as_ref(), req.news.as_ref());

        let changes = if stable_equal && context_eq && model_refs_eq {
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        } else {
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        };

        // Use Update (not UpdateReplace) — CREATE OR REPLACE handles in-place updates.
        let mut detailed_diff = HashMap::new();
        if changes == pulumirpc::diff_response::DiffChanges::DiffSome as i32 {
            detailed_diff.insert(
                "sql".to_owned(),
                pulumirpc::PropertyDiff {
                    kind: pulumirpc::property_diff::Kind::Update as i32,
                    input_diff: true,
                },
            );
        }

        Ok(Response::new(pulumirpc::DiffResponse {
            changes,
            replaces: Vec::new(), // No replaces — always in-place update.
            has_detailed_diff: true,
            detailed_diff,
            ..Default::default()
        }))
    }

    pub async fn create_dbt_model(
        &self,
        req: pulumirpc::CreateRequest,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let props = req
            .properties
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing properties"))?;

        let name = get_str(&props.fields, "name").unwrap_or("");
        let sql = get_str(&props.fields, "sql").unwrap_or("");
        let table_id_input = get_str(&props.fields, "tableId");
        let context = crate::prost_util::get_struct_fields(&props.fields, "context")
            .map(parse_project_context)
            .unwrap_or_else(|| crate::dbt::types::ProjectContext {
                gcp_project: "",
                dataset: "",
                sources: BTreeMap::new(),
                declared_models: vec![],
                declared_macros: vec![],
                vars: BTreeMap::new(),
            });
        let model_refs = parse_model_refs(&props.fields);
        let macros = parse_macro_defs(&props.fields);
        let max_bytes_billed = get_number(&props.fields, "maxBytesBilled").map(|n| n as i64);

        let table_ref = format!("`{}.{}.{}`", context.gcp_project, context.dataset, name);

        // Extract config from SQL.
        let mut config = extract_model_config(sql, &macros);
        let yaml_options = parse_resource_options(&props.fields);

        if let Some(ref yaml_opts) = yaml_options {
            // No need to re-run diff (check already ran), just override
            config.override_options(yaml_opts);
        }

        let materialization = &config.materialization;

        // Preprocess SQL for CREATE DDL (is_incremental = false).
        let preprocessed = preprocess(sql, &context.vars, &table_ref, false)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Resolve SQL.
        let resolved = resolve(
            &preprocessed,
            context.gcp_project,
            context.dataset,
            &context.sources,
            &model_refs,
            &macros,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Generate DDL based on whether tableId is provided.
        let has_table_id = table_id_input.is_some();
        let ddl = if has_table_id && materialization == "table" {
            // Mode 2: table pre-exists, use MERGE-based replacement.
            generate_merge_replace_ddl(context.gcp_project, context.dataset, name, &resolved)
        } else if has_table_id && materialization == "incremental" {
            // Mode 2 incremental: table pre-exists, skip CREATE — go straight to scheduler.
            String::new()
        } else {
            generate_ddl(
                context.gcp_project,
                context.dataset,
                name,
                materialization,
                &resolved,
                config.partition_by.as_ref(),
                config.cluster_by.as_deref(),
                Some(&config.to_table_options()),
            )
        };

        // Generate scheduler DDL for incremental models.
        let scheduler_sql = if materialization == "incremental" {
            // Preprocess with is_incremental = true.
            let preprocessed_inc = preprocess(sql, &context.vars, &table_ref, true)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let resolved_inc = resolve(
                &preprocessed_inc,
                context.gcp_project,
                context.dataset,
                &context.sources,
                &model_refs,
                &macros,
            )
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let unique_keys = config
                .unique_key_list
                .as_ref()
                .cloned()
                .or_else(|| config.unique_key.as_ref().map(|k| vec![k.clone()]))
                .unwrap_or_default();

            let strategy = config.incremental_strategy.as_deref().unwrap_or("merge");

            match strategy {
                "merge" => {
                    if !req.preview && !unique_keys.is_empty() {
                        // Get actual columns from table for proper UPDATE SET.
                        match self
                            .client
                            .get_table_schema(context.gcp_project, context.dataset, name)
                            .await
                        {
                            Ok(schema) => {
                                let columns: Vec<String> =
                                    schema.iter().map(|f| f.name.clone()).collect();
                                generate_merge_ddl(
                                    context.gcp_project,
                                    context.dataset,
                                    name,
                                    &unique_keys,
                                    &columns,
                                    &resolved_inc,
                                )
                            }
                            Err(_) => {
                                // Table might not exist yet (first create) — use placeholder.
                                generate_merge_ddl_placeholder(
                                    context.gcp_project,
                                    context.dataset,
                                    name,
                                    &unique_keys,
                                    &resolved_inc,
                                )
                            }
                        }
                    } else {
                        generate_merge_ddl_placeholder(
                            context.gcp_project,
                            context.dataset,
                            name,
                            &unique_keys,
                            &resolved_inc,
                        )
                    }
                }
                "delete+insert" => generate_delete_insert_ddl(
                    context.gcp_project,
                    context.dataset,
                    name,
                    &unique_keys,
                    &resolved_inc,
                ),
                "append" => {
                    generate_append_ddl(context.gcp_project, context.dataset, name, &resolved_inc)
                }
                _ => generate_merge_ddl_placeholder(
                    context.gcp_project,
                    context.dataset,
                    name,
                    &unique_keys,
                    &resolved_inc,
                ),
            }
        } else {
            ddl.clone()
        };

        let workflow_yaml = if scheduler_sql.is_empty() {
            String::new()
        } else {
            generate_workflow_yaml(context.gcp_project, &scheduler_sql)
        };

        // Execute DDL if non-ephemeral and not preview.
        if materialization != "ephemeral" && !req.preview && !ddl.is_empty() {
            self.client
                .execute_ddl(context.gcp_project, &ddl, max_bytes_billed)
                .await
                .status_internal()?;
        }

        // Feature 9: Validate scheduler MERGE SQL via dry-run.
        if !req.preview && materialization == "incremental" && !scheduler_sql.is_empty() {
            let dry_run = self
                .client
                .dry_run_query(context.gcp_project, &scheduler_sql, max_bytes_billed)
                .await
                .map_err(|e| {
                    Status::invalid_argument(format!(
                        "MERGE SQL validation failed (would fail at scheduler runtime): {}",
                        e
                    ))
                })?;
            if !dry_run.valid {
                return Err(Status::invalid_argument(format!(
                    "Generated MERGE SQL is invalid — scheduler job would fail at runtime: {}",
                    dry_run.error_message.unwrap_or_default()
                )));
            }
        }

        // Feature 10: Schema contract validation via dry-run.
        if !req.preview && materialization != "ephemeral" {
            let select_sql = resolved.to_sql();
            if let Ok(dry_run) = self
                .client
                .dry_run_query(context.gcp_project, &select_sql, max_bytes_billed)
                .await
            {
                if dry_run.valid && !dry_run.schema.is_empty() {
                    // Get declared schema if tableId is provided.
                    let declared_schema = if has_table_id {
                        self.client
                            .get_table_schema(context.gcp_project, context.dataset, name)
                            .await
                            .ok()
                    } else {
                        None
                    };
                    let schema_errors =
                        validate_model_schema(&dry_run.schema, declared_schema.as_deref(), &config);
                    if !schema_errors.is_empty() {
                        return Err(Status::invalid_argument(format!(
                            "Schema contract validation failed:\n{}",
                            schema_errors
                                .iter()
                                .map(|e| format!("  - {}", e))
                                .collect::<Vec<_>>()
                                .join("\n")
                        )));
                    }
                }
            }
        }

        let resolved_ctes_json = serde_json::to_string(&resolved.ctes).unwrap_or_default();
        let resolved_sql = resolved.to_sql().into_owned();
        let resolved_sql_prost = prost_string(&resolved_sql);

        // Cost estimation via dry-run (during preview or actual create).
        let (estimated_bytes_processed, estimated_cost) =
            if materialization != "ephemeral" && !resolved_sql.is_empty() {
                match self
                    .client
                    .dry_run_query(context.gcp_project, &resolved_sql, max_bytes_billed)
                    .await
                {
                    Ok(dr) if dr.valid && dr.total_bytes_processed > 0 => {
                        let cost = format_cost_estimate(dr.total_bytes_processed);
                        (Some(dr.total_bytes_processed), Some(cost))
                    }
                    _ => (None, None),
                }
            } else {
                (None, None)
            };

        let id = format!(
            "dbt-model/{}/{}/{}",
            context.gcp_project, context.dataset, name
        );

        // Build prost values that borrow before moving into ModelOutput.
        let materialization_prost = prost_string(materialization);
        let scheduler_sql_prost = prost_string(&scheduler_sql);

        let model_output = ModelOutput {
            materialization: materialization.to_owned(),
            resolved_ctes_json,
            resolved_body: resolved.body,
            table_ref,
            resolved_ddl: ddl,
            resolved_sql,
            workflow_yaml,
            table_id: name.to_owned(),
            scheduler_sql,
            estimated_bytes_processed,
            estimated_cost,
            max_bytes_billed,
        };

        let mut output_fields = BTreeMap::new();
        output_fields.insert("name".to_owned(), prost_string(name));
        output_fields.insert("sql".to_owned(), prost_string(sql));
        output_fields.insert("materialization".to_owned(), materialization_prost);
        output_fields.insert(
            "modelOutput".to_owned(),
            prost_struct(build_model_output_struct(&model_output).fields),
        );
        // Top-level convenience outputs.
        output_fields.insert("tableId".to_owned(), prost_string(name));
        output_fields.insert("schedulerSql".to_owned(), scheduler_sql_prost);
        output_fields.insert("resolvedSql".to_owned(), resolved_sql_prost);
        // Persist context so delete/read handlers can locate the BigQuery resource.
        if let Some(ctx_struct) = req
            .properties
            .as_ref()
            .and_then(|p| p.fields.get("context"))
        {
            output_fields.insert("context".to_owned(), ctx_struct.clone());
        }

        Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn read_dbt_model(
        &self,
        req: pulumirpc::ReadRequest,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let props = req.properties.as_ref();
        let materialization = props
            .and_then(|p| get_str(&p.fields, "materialization"))
            .unwrap_or("table");
        let name = props.and_then(|p| get_str(&p.fields, "name")).unwrap_or("");

        // Ephemeral models have no table — pass through.
        if materialization == "ephemeral" || name.is_empty() {
            return Ok(Response::new(pulumirpc::ReadResponse {
                id: req.id,
                properties: req.properties,
                inputs: req.inputs,
                ..Default::default()
            }));
        }

        // Verify table still exists in BigQuery.
        let context = props
            .and_then(|p| crate::prost_util::get_struct_fields(&p.fields, "context"))
            .or_else(|| {
                req.inputs
                    .as_ref()
                    .and_then(|i| crate::prost_util::get_struct_fields(&i.fields, "context"))
            })
            .map(parse_project_context);

        if let Some(ctx) = context {
            if !ctx.gcp_project.is_empty() && !ctx.dataset.is_empty() {
                match self
                    .client
                    .get_table(ctx.gcp_project, ctx.dataset, name)
                    .await
                {
                    Ok(_) => {
                        // Table exists — return stored state.
                    }
                    Err(_) => {
                        // Table gone — return empty ID to signal deletion.
                        return Ok(Response::new(pulumirpc::ReadResponse {
                            id: String::new(),
                            ..Default::default()
                        }));
                    }
                }
            }
        }

        Ok(Response::new(pulumirpc::ReadResponse {
            id: req.id,
            properties: req.properties,
            inputs: req.inputs,
            ..Default::default()
        }))
    }

    pub async fn update_dbt_model(
        &self,
        req: pulumirpc::UpdateRequest,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;

        let name = get_str(&news.fields, "name").unwrap_or("");
        let sql = get_str(&news.fields, "sql").unwrap_or("");
        let table_id_input = get_str(&news.fields, "tableId");
        let context = crate::prost_util::get_struct_fields(&news.fields, "context")
            .map(parse_project_context)
            .unwrap_or_else(|| crate::dbt::types::ProjectContext {
                gcp_project: "",
                dataset: "",
                sources: BTreeMap::new(),
                declared_models: vec![],
                declared_macros: vec![],
                vars: BTreeMap::new(),
            });
        let model_refs = parse_model_refs(&news.fields);
        let macros = parse_macro_defs(&news.fields);
        let max_bytes_billed = get_number(&news.fields, "maxBytesBilled").map(|n| n as i64);

        let table_ref = format!("`{}.{}.{}`", context.gcp_project, context.dataset, name);

        let mut config = extract_model_config(sql, &macros);
        let yaml_options = parse_resource_options(&news.fields);
        if let Some(ref yaml_opts) = yaml_options {
            config.override_options(yaml_opts);
        }

        let materialization = &config.materialization;

        let preprocessed = preprocess(sql, &context.vars, &table_ref, false)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let resolved = resolve(
            &preprocessed,
            context.gcp_project,
            context.dataset,
            &context.sources,
            &model_refs,
            &macros,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let has_table_id = table_id_input.is_some();
        let ddl = if has_table_id && materialization == "table" {
            generate_merge_replace_ddl(context.gcp_project, context.dataset, name, &resolved)
        } else if has_table_id && materialization == "incremental" {
            String::new()
        } else {
            generate_ddl(
                context.gcp_project,
                context.dataset,
                name,
                materialization,
                &resolved,
                config.partition_by.as_ref(),
                config.cluster_by.as_deref(),
                Some(&config.to_table_options()),
            )
        };

        // Generate scheduler DDL for incremental models.
        let scheduler_sql = if materialization == "incremental" {
            let preprocessed_inc = preprocess(sql, &context.vars, &table_ref, true)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let resolved_inc = resolve(
                &preprocessed_inc,
                context.gcp_project,
                context.dataset,
                &context.sources,
                &model_refs,
                &macros,
            )
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let unique_keys = config
                .unique_key_list
                .as_ref()
                .cloned()
                .or_else(|| config.unique_key.as_ref().map(|k| vec![k.clone()]))
                .unwrap_or_default();

            let strategy = config.incremental_strategy.as_deref().unwrap_or("merge");

            match strategy {
                "merge" => {
                    if !req.preview && !unique_keys.is_empty() {
                        match self
                            .client
                            .get_table_schema(context.gcp_project, context.dataset, name)
                            .await
                        {
                            Ok(schema) => {
                                let columns: Vec<String> =
                                    schema.iter().map(|f| f.name.clone()).collect();
                                generate_merge_ddl(
                                    context.gcp_project,
                                    context.dataset,
                                    name,
                                    &unique_keys,
                                    &columns,
                                    &resolved_inc,
                                )
                            }
                            Err(_) => generate_merge_ddl_placeholder(
                                context.gcp_project,
                                context.dataset,
                                name,
                                &unique_keys,
                                &resolved_inc,
                            ),
                        }
                    } else {
                        generate_merge_ddl_placeholder(
                            context.gcp_project,
                            context.dataset,
                            name,
                            &unique_keys,
                            &resolved_inc,
                        )
                    }
                }
                "delete+insert" => generate_delete_insert_ddl(
                    context.gcp_project,
                    context.dataset,
                    name,
                    &unique_keys,
                    &resolved_inc,
                ),
                "append" => {
                    generate_append_ddl(context.gcp_project, context.dataset, name, &resolved_inc)
                }
                _ => generate_merge_ddl_placeholder(
                    context.gcp_project,
                    context.dataset,
                    name,
                    &unique_keys,
                    &resolved_inc,
                ),
            }
        } else {
            ddl.clone()
        };

        let workflow_yaml = if scheduler_sql.is_empty() {
            String::new()
        } else {
            generate_workflow_yaml(context.gcp_project, &scheduler_sql)
        };

        // Execute DDL if non-ephemeral and not preview.
        if materialization != "ephemeral" && !req.preview && !ddl.is_empty() {
            self.client
                .execute_ddl(context.gcp_project, &ddl, max_bytes_billed)
                .await
                .status_internal()?;
        }

        let resolved_ctes_json = serde_json::to_string(&resolved.ctes).unwrap_or_default();
        let resolved_sql = resolved.to_sql().into_owned();
        let resolved_sql_prost = prost_string(&resolved_sql);
        let materialization_prost = prost_string(materialization);
        let scheduler_sql_prost = prost_string(&scheduler_sql);

        // Cost estimation via dry-run.
        let (estimated_bytes_processed, estimated_cost) =
            if materialization != "ephemeral" && !resolved_sql.is_empty() {
                match self
                    .client
                    .dry_run_query(context.gcp_project, &resolved_sql, max_bytes_billed)
                    .await
                {
                    Ok(dr) if dr.valid && dr.total_bytes_processed > 0 => {
                        let cost = format_cost_estimate(dr.total_bytes_processed);
                        (Some(dr.total_bytes_processed), Some(cost))
                    }
                    _ => (None, None),
                }
            } else {
                (None, None)
            };

        let model_output = ModelOutput {
            materialization: materialization.to_owned(),
            resolved_ctes_json,
            resolved_body: resolved.body,
            table_ref,
            resolved_ddl: ddl,
            resolved_sql,
            workflow_yaml,
            table_id: name.to_owned(),
            scheduler_sql,
            estimated_bytes_processed,
            estimated_cost,
            max_bytes_billed,
        };

        let mut output_fields = BTreeMap::new();
        output_fields.insert("name".to_owned(), prost_string(name));
        output_fields.insert("sql".to_owned(), prost_string(sql));
        output_fields.insert("materialization".to_owned(), materialization_prost);
        output_fields.insert(
            "modelOutput".to_owned(),
            prost_struct(build_model_output_struct(&model_output).fields),
        );
        output_fields.insert("tableId".to_owned(), prost_string(name));
        output_fields.insert("schedulerSql".to_owned(), scheduler_sql_prost);
        output_fields.insert("resolvedSql".to_owned(), resolved_sql_prost);
        if let Some(ctx_struct) = req.news.as_ref().and_then(|p| p.fields.get("context")) {
            output_fields.insert("context".to_owned(), ctx_struct.clone());
        }

        Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(prost_types::Struct {
                fields: output_fields,
            }),
            ..Default::default()
        }))
    }

    pub async fn delete_dbt_model(
        &self,
        req: pulumirpc::DeleteRequest,
    ) -> Result<Response<()>, Status> {
        let materialization = req
            .properties
            .as_ref()
            .and_then(|p| get_str(&p.fields, "materialization"))
            .unwrap_or("table");

        if materialization == "ephemeral" {
            return Ok(Response::new(()));
        }

        if let Some(ref props) = req.properties {
            // If tableId was provided as input, table is managed by Table resource — don't delete.
            let table_id_input = req
                .old_inputs
                .as_ref()
                .and_then(|i| get_str(&i.fields, "tableId"));
            if table_id_input.is_some() {
                return Ok(Response::new(()));
            }

            // Try context from properties first, fall back to old_inputs.
            let context = crate::prost_util::get_struct_fields(&props.fields, "context")
                .or_else(|| {
                    req.old_inputs
                        .as_ref()
                        .and_then(|i| crate::prost_util::get_struct_fields(&i.fields, "context"))
                })
                .map(parse_project_context);
            let name = get_str(&props.fields, "name").unwrap_or("");

            if let Some(ctx) = context {
                if !ctx.gcp_project.is_empty() && !ctx.dataset.is_empty() && !name.is_empty() {
                    let _ = self
                        .client
                        .delete_table(ctx.gcp_project, ctx.dataset, name)
                        .await;
                }
            }
        }

        Ok(Response::new(()))
    }
}

/// Parse the nested `options` struct from Pulumi resource props into `OwnedTableOptions`.
/// Returns `None` if the `options` key is absent (user didn't declare YAML options).
fn parse_resource_options(
    fields: &BTreeMap<String, prost_types::Value>,
) -> Option<OwnedTableOptions> {
    let opts = get_struct_fields(fields, "options")?;
    let mut labels = std::collections::BTreeMap::new();
    if let Some(label_fields) = get_struct_fields(opts, "labels") {
        for (k, v) in label_fields {
            if let Some(s) = crate::prost_util::value_as_str(v) {
                labels.insert(k.clone(), s.to_owned());
            }
        }
    }
    Some(OwnedTableOptions {
        require_partition_filter: get_bool(opts, "requirePartitionFilter"),
        partition_expiration_days: get_number(opts, "partitionExpirationDays").map(|n| n as u32),
        friendly_name: get_str(opts, "friendlyName").map(|s| s.to_owned()),
        description: get_str(opts, "description").map(|s| s.to_owned()),
        labels,
        kms_key_name: get_str(opts, "kmsKeyName").map(|s| s.to_owned()),
        default_collation_name: get_str(opts, "defaultCollationName").map(|s| s.to_owned()),
        enable_refresh: get_bool(opts, "enableRefresh"),
        refresh_interval_minutes: get_number(opts, "refreshIntervalMinutes").map(|n| n as u32),
        max_staleness: get_str(opts, "maxStaleness").map(|s| s.to_owned()),
    })
}

/// Compare only specified input fields between olds and news structs.
fn fields_equal(
    olds: Option<&prost_types::Struct>,
    news: Option<&prost_types::Struct>,
    keys: &[&str],
) -> bool {
    match (olds, news) {
        (Some(o), Some(n)) => keys.iter().all(|k| o.fields.get(*k) == n.fields.get(*k)),
        (None, None) => true,
        _ => false,
    }
}

/// Compare only semantically meaningful context fields (gcpProject, dataset).
/// Ignores structural noise like extra fields from re-reads.
fn context_fields_equal(
    olds: Option<&prost_types::Struct>,
    news: Option<&prost_types::Struct>,
) -> bool {
    let old_ctx = olds.and_then(|o| get_struct_fields(&o.fields, "context"));
    let new_ctx = news.and_then(|n| get_struct_fields(&n.fields, "context"));
    match (old_ctx, new_ctx) {
        (Some(o), Some(n)) => {
            get_str(o, "gcpProject") == get_str(n, "gcpProject")
                && get_str(o, "dataset") == get_str(n, "dataset")
        }
        (None, None) => true,
        _ => false,
    }
}

/// Compare only the tableRef field from each model in modelRefs.
/// Ignores computed fields like resolvedSql, resolvedDdl, workflowYaml.
fn model_refs_equal(
    olds: Option<&prost_types::Struct>,
    news: Option<&prost_types::Struct>,
) -> bool {
    fn extract_table_refs(s: &prost_types::Struct) -> Vec<String> {
        let Some(refs_val) = s.fields.get("modelRefs") else {
            return Vec::new();
        };
        let Some(prost_types::value::Kind::StructValue(refs_struct)) = refs_val.kind.as_ref()
        else {
            return Vec::new();
        };
        let mut refs: Vec<String> = refs_struct
            .fields
            .values()
            .map(|v| {
                if let Some(prost_types::value::Kind::StructValue(inner)) = v.kind.as_ref() {
                    get_str(&inner.fields, "tableRef").unwrap_or("").to_owned()
                } else {
                    String::new()
                }
            })
            .collect();
        refs.sort();
        refs
    }

    match (olds, news) {
        (Some(o), Some(n)) => extract_table_refs(o) == extract_table_refs(n),
        (None, None) => true,
        _ => false,
    }
}

/// Extract materialization and config values from SQL config block.
pub fn extract_model_config(
    sql: &str,
    macros: &BTreeMap<String, crate::dbt::macros::MacroDef>,
) -> ModelConfig {
    let expanded =
        crate::dbt::macros::expand_macros(sql, macros).unwrap_or_else(|_| sql.to_owned());
    let mut materialization = None;
    let mut unique_key = None;
    let mut unique_key_list = None;
    let mut incremental_strategy = None;
    let mut partition_by = None;
    let mut cluster_by = None;
    let mut require_partition_filter = None;
    let mut partition_expiration_days = None;
    let mut friendly_name = None;
    let mut description = None;
    let mut labels = Vec::new();
    let mut kms_key_name = None;
    let mut default_collation_name = None;
    let mut enable_refresh = None;
    let mut refresh_interval_minutes = None;
    let mut max_staleness = None;

    for segment in DbtScanner::new(&expanded) {
        if let DbtSegment::Config { raw_args } = segment {
            for entry in ConfigArgIter::new(raw_args) {
                match entry.key {
                    "materialized" => {
                        if let ConfigValue::Str(v) = entry.value {
                            materialization = Some(v.to_owned());
                        }
                    }
                    "unique_key" => match entry.value {
                        ConfigValue::Str(v) => {
                            unique_key = Some(v.to_owned());
                        }
                        ConfigValue::List(items) => {
                            unique_key_list = Some(items.iter().map(|s| s.to_string()).collect());
                        }
                        _ => {}
                    },
                    "incremental_strategy" => {
                        if let ConfigValue::Str(v) = entry.value {
                            incremental_strategy = Some(v.to_owned());
                        }
                    }
                    "partition_by" => {
                        if let ConfigValue::Dict(pairs) = entry.value {
                            let field = pairs
                                .iter()
                                .find(|(k, _)| *k == "field")
                                .map(|(_, v)| v.to_string())
                                .unwrap_or_default();
                            let data_type = pairs
                                .iter()
                                .find(|(k, _)| *k == "data_type")
                                .map(|(_, v)| v.to_string())
                                .unwrap_or_default();
                            let granularity = pairs
                                .iter()
                                .find(|(k, _)| *k == "granularity")
                                .map(|(_, v)| v.to_string());
                            partition_by = Some(PartitionConfig {
                                field,
                                data_type,
                                granularity,
                            });
                        }
                    }
                    "cluster_by" => {
                        if let ConfigValue::List(items) = entry.value {
                            cluster_by = Some(items.iter().map(|s| s.to_string()).collect());
                        }
                    }
                    "require_partition_filter" => match entry.value {
                        ConfigValue::Bool(b) => require_partition_filter = Some(b),
                        ConfigValue::Str(v) => {
                            require_partition_filter = Some(v.eq_ignore_ascii_case("true"));
                        }
                        _ => {}
                    },
                    "partition_expiration_days" => {
                        if let ConfigValue::Str(v) = entry.value {
                            partition_expiration_days = v.parse().ok();
                        }
                    }
                    "friendly_name" => {
                        if let ConfigValue::Str(v) = entry.value {
                            friendly_name = Some(v.to_owned());
                        }
                    }
                    "description" => {
                        if let ConfigValue::Str(v) = entry.value {
                            description = Some(v.to_owned());
                        }
                    }
                    "labels" => {
                        if let ConfigValue::Dict(pairs) = entry.value {
                            labels = pairs
                                .iter()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                        }
                    }
                    "kms_key_name" => {
                        if let ConfigValue::Str(v) = entry.value {
                            kms_key_name = Some(v.to_owned());
                        }
                    }
                    "default_collation_name" => {
                        if let ConfigValue::Str(v) = entry.value {
                            default_collation_name = Some(v.to_owned());
                        }
                    }
                    "enable_refresh" => match entry.value {
                        ConfigValue::Bool(b) => enable_refresh = Some(b),
                        ConfigValue::Str(v) => {
                            enable_refresh = Some(v.eq_ignore_ascii_case("true"));
                        }
                        _ => {}
                    },
                    "refresh_interval_minutes" => {
                        if let ConfigValue::Str(v) = entry.value {
                            refresh_interval_minutes = v.parse().ok();
                        }
                    }
                    "max_staleness" => {
                        if let ConfigValue::Str(v) = entry.value {
                            max_staleness = Some(v.to_owned());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    ModelConfig {
        materialization: materialization.unwrap_or_else(|| "table".to_owned()),
        unique_key,
        unique_key_list,
        incremental_strategy,
        partition_by,
        cluster_by,
        require_partition_filter,
        partition_expiration_days,
        friendly_name,
        description,
        labels,
        kms_key_name,
        default_collation_name,
        enable_refresh,
        refresh_interval_minutes,
        max_staleness,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp_client::MockGcpClient;
    use crate::prost_util::{prost_list, prost_string, prost_struct};

    fn mock_provider() -> GcpxProvider<MockGcpClient> {
        GcpxProvider::new(MockGcpClient::new(vec![]))
    }

    fn make_project_inputs(gcp_project: &str, dataset: &str) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("gcpProject".to_owned(), prost_string(gcp_project)),
                ("dataset".to_owned(), prost_string(dataset)),
                ("declaredModels".to_owned(), prost_list(vec![])),
                ("declaredMacros".to_owned(), prost_list(vec![])),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn make_macro_inputs(name: &str, sql: &str, args: Vec<&str>) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                (
                    "args".to_owned(),
                    prost_list(args.iter().map(|a| prost_string(a)).collect()),
                ),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn make_model_inputs(name: &str, sql: &str, ctx: prost_types::Value) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                ("context".to_owned(), ctx),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn make_context_value(
        gcp_project: &str,
        dataset: &str,
        models: Vec<&str>,
    ) -> prost_types::Value {
        let mut fields = BTreeMap::new();
        fields.insert("gcpProject".to_owned(), prost_string(gcp_project));
        fields.insert("dataset".to_owned(), prost_string(dataset));
        fields.insert(
            "declaredModels".to_owned(),
            prost_list(models.iter().map(|m| prost_string(m)).collect()),
        );
        fields.insert("declaredMacros".to_owned(), prost_list(vec![]));
        prost_struct(fields)
    }

    // === Project handler tests ===

    #[tokio::test]
    async fn check_project_valid() {
        let p = mock_provider();
        let news = make_project_inputs("proj", "ds");
        let resp = p
            .check_dbt_project(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_project_empty_gcp_project() {
        let p = mock_provider();
        let news = make_project_inputs("", "ds");
        let resp = p
            .check_dbt_project(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(!failures.is_empty());
        assert!(failures.iter().any(|f| f.property == "gcpProject"));
    }

    #[tokio::test]
    async fn create_project_outputs_context() {
        let p = mock_provider();
        let props = make_project_inputs("proj", "ds");
        let resp = p
            .create_dbt_project(pulumirpc::CreateRequest {
                properties: Some(props),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "dbt-project/proj/ds");
        let output = inner.properties.unwrap();
        assert!(output.fields.contains_key("context"));
    }

    #[tokio::test]
    async fn diff_project_no_change() {
        let p = mock_provider();
        let inputs = make_project_inputs("proj", "ds");
        let resp = p
            .diff_dbt_project(pulumirpc::DiffRequest {
                olds: Some(inputs.clone()),
                news: Some(inputs),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().changes,
            pulumirpc::diff_response::DiffChanges::DiffNone as i32
        );
    }

    #[tokio::test]
    async fn diff_project_change() {
        let p = mock_provider();
        let olds = make_project_inputs("proj1", "ds");
        let news = make_project_inputs("proj2", "ds");
        let resp = p
            .diff_dbt_project(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
    }

    #[tokio::test]
    async fn read_project_passthrough() {
        let p = mock_provider();
        let props = make_project_inputs("proj", "ds");
        let resp = p
            .read_dbt_project(pulumirpc::ReadRequest {
                id: "dbt-project/proj/ds".into(),
                properties: Some(props.clone()),
                inputs: Some(props),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "dbt-project/proj/ds");
        assert!(inner.properties.is_some());
    }

    #[tokio::test]
    async fn update_project_rebuilds_context() {
        let p = mock_provider();
        let news = make_project_inputs("proj", "new_ds");
        let resp = p
            .update_dbt_project(pulumirpc::UpdateRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        assert_eq!(get_str(&output.fields, "dataset"), Some("new_ds"));
        assert!(output.fields.contains_key("context"));
    }

    #[tokio::test]
    async fn delete_project_noop() {
        let p = mock_provider();
        let resp = p
            .delete_dbt_project(pulumirpc::DeleteRequest::default())
            .await;
        assert!(resp.is_ok());
    }

    // === Macro handler tests ===

    #[tokio::test]
    async fn check_macro_valid() {
        let p = mock_provider();
        let news = make_macro_inputs("sk", "SELECT {{ x }}", vec!["x"]);
        let resp = p
            .check_dbt_macro(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_macro_reserved_name() {
        let p = mock_provider();
        let news = make_macro_inputs("ref", "SELECT 1", vec![]);
        let resp = p
            .check_dbt_macro(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(!failures.is_empty());
        assert!(failures.iter().any(|f| f.reason.contains("reserved")));
    }

    #[tokio::test]
    async fn create_macro_outputs_macro_output() {
        let p = mock_provider();
        let props = make_macro_inputs("sk", "MD5({{ x }})", vec!["x"]);
        let resp = p
            .create_dbt_macro(pulumirpc::CreateRequest {
                properties: Some(props),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "dbt-macro/sk");
        let output = inner.properties.unwrap();
        assert!(output.fields.contains_key("macroOutput"));
    }

    #[tokio::test]
    async fn diff_macro_change() {
        let p = mock_provider();
        let olds = make_macro_inputs("m", "SELECT 1", vec![]);
        let news = make_macro_inputs("m", "SELECT 2", vec![]);
        let resp = p
            .diff_dbt_macro(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
    }

    // === Model handler tests ===

    #[tokio::test]
    async fn check_model_valid() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let news = make_model_inputs("m", "{{ config(materialized='table') }} SELECT 1", ctx);
        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_model_no_config_is_valid() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let news = make_model_inputs("m", "SELECT 1", ctx);
        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_model_unknown_ref() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let news = make_model_inputs(
            "m",
            "{{ config(materialized='table') }} SELECT * FROM {{ ref('missing') }}",
            ctx,
        );
        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("references unknown model")));
    }

    #[tokio::test]
    async fn create_model_table_executes_ddl() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs("m", "{{ config(materialized='table') }} SELECT 1", ctx);
        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.id, "dbt-model/p/d/m");

        let ddl_log = p.client.ddl_log();
        assert!(!ddl_log.is_empty());
        assert!(ddl_log[0].contains("CREATE OR REPLACE TABLE"));
    }

    #[tokio::test]
    async fn create_model_ephemeral_no_ddl() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs("m", "{{ config(materialized='ephemeral') }} SELECT 1", ctx);
        p.create_dbt_model(pulumirpc::CreateRequest {
            properties: Some(props),
            preview: false,
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(p.client.ddl_log().is_empty());
    }

    #[tokio::test]
    async fn create_model_preview_no_ddl() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs("m", "{{ config(materialized='table') }} SELECT 1", ctx);
        p.create_dbt_model(pulumirpc::CreateRequest {
            properties: Some(props),
            preview: true,
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(p.client.ddl_log().is_empty());
    }

    #[tokio::test]
    async fn create_model_outputs_model_output() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs("m", "{{ config(materialized='view') }} SELECT 1", ctx);
        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        assert!(output.fields.contains_key("modelOutput"));
        assert_eq!(get_str(&output.fields, "materialization"), Some("view"));

        // Verify modelOutput has all 9 fields.
        let mo = crate::prost_util::get_struct_fields(&output.fields, "modelOutput").unwrap();
        assert!(mo.contains_key("materialization"));
        assert!(mo.contains_key("resolvedCtesJson"));
        assert!(mo.contains_key("resolvedBody"));
        assert!(mo.contains_key("tableRef"));
        assert!(mo.contains_key("resolvedDdl"));
        assert!(mo.contains_key("resolvedSql"));
        assert!(mo.contains_key("workflowYaml"));
        assert!(mo.contains_key("tableId"));
        assert!(mo.contains_key("schedulerSql"));

        // Verify top-level convenience outputs.
        assert!(output.fields.contains_key("tableId"));
        assert!(output.fields.contains_key("schedulerSql"));
        assert!(output.fields.contains_key("resolvedSql"));
    }

    #[tokio::test]
    async fn create_model_with_macros() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        let mut macro_fields = BTreeMap::new();
        macro_fields.insert("args".to_owned(), prost_list(vec![prost_string("cols")]));
        macro_fields.insert("sql".to_owned(), prost_string("MD5({{ cols }})"));

        let mut macros_map = BTreeMap::new();
        macros_map.insert("sk".to_owned(), prost_struct(macro_fields));

        let mut props_fields = BTreeMap::new();
        props_fields.insert("name".to_owned(), prost_string("m"));
        props_fields.insert(
            "sql".to_owned(),
            prost_string("{{ config(materialized='table') }} SELECT {{ sk('id') }}"),
        );
        props_fields.insert("context".to_owned(), ctx);
        props_fields.insert("macros".to_owned(), prost_struct(macros_map));

        let props = prost_types::Struct {
            fields: props_fields,
        };

        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        let mo = crate::prost_util::get_struct_fields(&output.fields, "modelOutput").unwrap();
        let resolved_sql = get_str(mo, "resolvedSql").unwrap();
        assert!(resolved_sql.contains("MD5(id)"));
    }

    #[tokio::test]
    async fn diff_model_always_replace() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let olds = make_model_inputs(
            "m",
            "{{ config(materialized='table') }} SELECT 1",
            ctx.clone(),
        );
        let news = make_model_inputs("m", "{{ config(materialized='table') }} SELECT 2", ctx);
        let resp = p
            .diff_dbt_model(pulumirpc::DiffRequest {
                olds: Some(olds),
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert_eq!(
            inner.changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32
        );
        // Update (not replace) — CREATE OR REPLACE handles in-place.
        assert!(inner.replaces.is_empty());
        assert!(inner.detailed_diff.contains_key("sql"));
        assert_eq!(
            inner.detailed_diff["sql"].kind,
            pulumirpc::property_diff::Kind::Update as i32
        );
    }

    #[tokio::test]
    async fn delete_model_table_deletes() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let mut fields = BTreeMap::new();
        fields.insert("name".to_owned(), prost_string("m"));
        fields.insert("materialization".to_owned(), prost_string("table"));
        fields.insert("context".to_owned(), ctx);

        p.delete_dbt_model(pulumirpc::DeleteRequest {
            id: "dbt-model/p/d/m".into(),
            properties: Some(prost_types::Struct {
                fields: fields.clone(),
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        let table_log = p.client.table_log.lock().unwrap().clone();
        assert!(table_log.iter().any(|(op, _)| op == "delete"));
    }

    #[tokio::test]
    async fn delete_model_ephemeral_noop() {
        let p = mock_provider();
        let mut fields = BTreeMap::new();
        fields.insert("name".to_owned(), prost_string("m"));
        fields.insert("materialization".to_owned(), prost_string("ephemeral"));

        p.delete_dbt_model(pulumirpc::DeleteRequest {
            id: "dbt-model/p/d/m".into(),
            properties: Some(prost_types::Struct { fields }),
            ..Default::default()
        })
        .await
        .unwrap();

        let table_log = p.client.table_log.lock().unwrap().clone();
        assert!(table_log.is_empty());
    }

    #[tokio::test]
    async fn update_model_executes_ddl() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let olds = make_model_inputs(
            "m",
            "{{ config(materialized='table') }} SELECT 1 AS id",
            ctx.clone(),
        );
        let news = make_model_inputs(
            "m",
            "{{ config(materialized='table') }} SELECT 2 AS id",
            ctx,
        );
        let result = p
            .update_dbt_model(pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            })
            .await;
        assert!(result.is_ok());
        let inner = result.unwrap().into_inner();
        assert!(inner.properties.is_some());
        // DDL should have been executed.
        let ddl_log = p.client.ddl_log();
        assert!(!ddl_log.is_empty());
        assert!(ddl_log[0].contains("CREATE OR REPLACE"));
    }

    // === New Feature tests ===

    #[tokio::test]
    async fn create_model_incremental_generates_merge_placeholder() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs(
            "m",
            "{{ config(materialized='incremental', unique_key='id') }} SELECT id, name FROM t",
            ctx,
        );
        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        let mo = crate::prost_util::get_struct_fields(&output.fields, "modelOutput").unwrap();
        let scheduler_sql = get_str(mo, "schedulerSql").unwrap();
        // Preview mode: should produce MERGE placeholder (INSERT-only).
        assert!(scheduler_sql.contains("MERGE"));
        assert!(scheduler_sql.contains("ON T.`id` = S.`id`"));

        // Top-level output should match.
        assert_eq!(get_str(&output.fields, "schedulerSql"), Some(scheduler_sql));
    }

    #[tokio::test]
    async fn create_model_with_partition_and_cluster() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let props = make_model_inputs(
            "m",
            "{{ config(materialized='table', partition_by={'field': 'ts', 'data_type': 'date'}, cluster_by=['id']) }} SELECT id, ts FROM t",
            ctx,
        );
        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        let mo = crate::prost_util::get_struct_fields(&output.fields, "modelOutput").unwrap();
        let ddl = get_str(mo, "resolvedDdl").unwrap();
        assert!(ddl.contains("PARTITION BY ts"));
        assert!(ddl.contains("CLUSTER BY id"));
    }

    #[tokio::test]
    async fn create_model_with_table_id_uses_merge_replace() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        let mut props_fields = BTreeMap::new();
        props_fields.insert("name".to_owned(), prost_string("m"));
        props_fields.insert(
            "sql".to_owned(),
            prost_string("{{ config(materialized='table') }} SELECT 1 AS id"),
        );
        props_fields.insert("context".to_owned(), ctx);
        props_fields.insert("tableId".to_owned(), prost_string("m"));

        let props = prost_types::Struct {
            fields: props_fields,
        };

        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        let ddl_log = p.client.ddl_log();
        assert!(!ddl_log.is_empty());
        // Should use MERGE ON FALSE instead of CREATE OR REPLACE TABLE.
        assert!(ddl_log[0].contains("ON FALSE"));
        assert!(!ddl_log[0].contains("CREATE OR REPLACE TABLE"));

        // Top-level tableId output.
        assert_eq!(get_str(&output.fields, "tableId"), Some("m"));
    }

    #[tokio::test]
    async fn delete_model_with_table_id_preserves_table() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let mut props_fields = BTreeMap::new();
        props_fields.insert("name".to_owned(), prost_string("m"));
        props_fields.insert("materialization".to_owned(), prost_string("table"));
        props_fields.insert("context".to_owned(), ctx);

        let mut input_fields = BTreeMap::new();
        input_fields.insert("tableId".to_owned(), prost_string("m"));

        p.delete_dbt_model(pulumirpc::DeleteRequest {
            id: "dbt-model/p/d/m".into(),
            properties: Some(prost_types::Struct {
                fields: props_fields,
            }),
            old_inputs: Some(prost_types::Struct {
                fields: input_fields,
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        // Should NOT delete — table is managed by Table resource.
        let table_log = p.client.table_log.lock().unwrap().clone();
        assert!(
            table_log.is_empty(),
            "expected no deletes when tableId is provided"
        );
    }

    #[tokio::test]
    async fn read_model_table_missing_returns_empty_id() {
        let p = GcpxProvider::new(MockGcpClient {
            fail_on: std::sync::Mutex::new(Some("get_table".to_owned())),
            ..MockGcpClient::new(vec![])
        });
        let ctx = make_context_value("p", "d", vec!["m"]);
        let mut fields = BTreeMap::new();
        fields.insert("name".to_owned(), prost_string("m"));
        fields.insert("materialization".to_owned(), prost_string("table"));
        fields.insert("context".to_owned(), ctx);

        let resp = p
            .read_dbt_model(pulumirpc::ReadRequest {
                id: "dbt-model/p/d/m".into(),
                properties: Some(prost_types::Struct { fields }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().id.is_empty());
    }

    #[tokio::test]
    async fn read_model_ephemeral_passthrough() {
        let p = mock_provider();
        let mut fields = BTreeMap::new();
        fields.insert("name".to_owned(), prost_string("m"));
        fields.insert("materialization".to_owned(), prost_string("ephemeral"));

        let resp = p
            .read_dbt_model(pulumirpc::ReadRequest {
                id: "dbt-model/p/d/m".into(),
                properties: Some(prost_types::Struct { fields }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.into_inner().id, "dbt-model/p/d/m");
    }

    #[tokio::test]
    async fn extract_config_incremental_with_all_options() {
        let sql = "{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge', partition_by={'field': 'ts', 'data_type': 'timestamp', 'granularity': 'day'}, cluster_by=['region', 'id']) }} SELECT 1";
        let config = extract_model_config(sql, &BTreeMap::new());
        assert_eq!(config.materialization, "incremental");
        assert_eq!(config.unique_key, Some("id".to_owned()));
        assert_eq!(config.incremental_strategy, Some("merge".to_owned()));
        let part = config.partition_by.unwrap();
        assert_eq!(part.field, "ts");
        assert_eq!(part.data_type, "timestamp");
        assert_eq!(part.granularity, Some("day".to_owned()));
        let cols = config.cluster_by.unwrap();
        assert_eq!(cols, vec!["region", "id"]);
    }

    #[tokio::test]
    async fn extract_config_composite_unique_key() {
        let sql =
            "{{ config(materialized='incremental', unique_key=['order_id', 'line_id']) }} SELECT 1";
        let config = extract_model_config(sql, &BTreeMap::new());
        assert!(config.unique_key.is_none());
        let keys = config.unique_key_list.unwrap();
        assert_eq!(keys, vec!["order_id", "line_id"]);
    }

    // === parse_resource_options tests ===

    use crate::prost_util::{prost_bool, prost_number};

    #[test]
    fn parse_resource_options_missing_returns_none() {
        let fields = BTreeMap::new();
        assert!(parse_resource_options(&fields).is_none());
    }

    #[test]
    fn parse_resource_options_empty_struct() {
        let mut fields = BTreeMap::new();
        fields.insert("options".to_owned(), prost_struct(BTreeMap::new()));
        let opts = parse_resource_options(&fields).unwrap();
        assert!(opts.require_partition_filter.is_none());
        assert!(opts.labels.is_empty());
    }

    #[test]
    fn parse_resource_options_full() {
        let mut label_fields = BTreeMap::new();
        label_fields.insert("env".to_owned(), prost_string("prod"));
        label_fields.insert("team".to_owned(), prost_string("analytics"));

        let mut opt_fields = BTreeMap::new();
        opt_fields.insert("requirePartitionFilter".to_owned(), prost_bool(true));
        opt_fields.insert("partitionExpirationDays".to_owned(), prost_number(90.0));
        opt_fields.insert("friendlyName".to_owned(), prost_string("My Table"));
        opt_fields.insert("description".to_owned(), prost_string("A test table"));
        opt_fields.insert("labels".to_owned(), prost_struct(label_fields));
        opt_fields.insert("kmsKeyName".to_owned(), prost_string("projects/p/key"));
        opt_fields.insert("defaultCollationName".to_owned(), prost_string("und:ci"));
        opt_fields.insert("enableRefresh".to_owned(), prost_bool(true));
        opt_fields.insert("refreshIntervalMinutes".to_owned(), prost_number(30.0));
        opt_fields.insert("maxStaleness".to_owned(), prost_string("0-0 0 4:0:0"));

        let mut fields = BTreeMap::new();
        fields.insert("options".to_owned(), prost_struct(opt_fields));

        let opts = parse_resource_options(&fields).unwrap();
        assert_eq!(opts.require_partition_filter, Some(true));
        assert_eq!(opts.partition_expiration_days, Some(90));
        assert_eq!(opts.friendly_name.as_deref(), Some("My Table"));
        assert_eq!(opts.description.as_deref(), Some("A test table"));
        assert_eq!(opts.labels.len(), 2);
        assert_eq!(opts.labels.get("env").map(|s| s.as_str()), Some("prod"));
        assert_eq!(opts.kms_key_name.as_deref(), Some("projects/p/key"));
        assert_eq!(opts.default_collation_name.as_deref(), Some("und:ci"));
        assert_eq!(opts.enable_refresh, Some(true));
        assert_eq!(opts.refresh_interval_minutes, Some(30));
        assert_eq!(opts.max_staleness.as_deref(), Some("0-0 0 4:0:0"));
    }

    // === Options diff integration tests ===

    #[tokio::test]
    async fn check_model_conflicting_options_returns_failures() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        let mut opt_fields = BTreeMap::new();
        // YAML says description = "YAML desc"
        opt_fields.insert("description".to_owned(), prost_string("YAML desc"));

        let mut news_fields = BTreeMap::new();
        news_fields.insert("name".to_owned(), prost_string("m"));
        // SQL says description='SQL desc'
        news_fields.insert(
            "sql".to_owned(),
            prost_string("{{ config(materialized='table', description='SQL desc') }} SELECT 1"),
        );
        news_fields.insert("context".to_owned(), ctx);
        news_fields.insert("options".to_owned(), prost_struct(opt_fields));

        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(prost_types::Struct {
                    fields: news_fields,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(!failures.is_empty(), "expected option conflict failures");
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("conflicting values")));
    }

    #[tokio::test]
    async fn check_model_sql_only_option_returns_failure() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        // YAML options declared but empty (no require_partition_filter)
        let opt_fields = BTreeMap::new();

        let mut news_fields = BTreeMap::new();
        news_fields.insert("name".to_owned(), prost_string("m"));
        news_fields.insert(
            "sql".to_owned(),
            prost_string(
                "{{ config(materialized='table', require_partition_filter=true, \
                 partition_by={'field': 'ts', 'data_type': 'date'}) }} SELECT 1",
            ),
        );
        news_fields.insert("context".to_owned(), ctx);
        news_fields.insert("options".to_owned(), prost_struct(opt_fields));

        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(prost_types::Struct {
                    fields: news_fields,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(!failures.is_empty());
        assert!(failures.iter().any(|f| f.reason.contains("not declared")));
    }

    #[tokio::test]
    async fn check_model_no_yaml_options_passes_through() {
        // When no YAML options declared, diff is not called — SQL options pass through.
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);
        let news = make_model_inputs(
            "m",
            "{{ config(materialized='table', description='SQL desc') }} SELECT 1",
            ctx,
        );
        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(news),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.into_inner().failures.is_empty());
    }

    #[tokio::test]
    async fn check_model_mode2_with_options_fails() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        let mut opt_fields = BTreeMap::new();
        opt_fields.insert("friendlyName".to_owned(), prost_string("My Table"));

        let mut news_fields = BTreeMap::new();
        news_fields.insert("name".to_owned(), prost_string("m"));
        news_fields.insert(
            "sql".to_owned(),
            prost_string("{{ config(materialized='table') }} SELECT 1"),
        );
        news_fields.insert("context".to_owned(), ctx);
        news_fields.insert("tableId".to_owned(), prost_string("m"));
        news_fields.insert("options".to_owned(), prost_struct(opt_fields));

        let resp = p
            .check_dbt_model(pulumirpc::CheckRequest {
                news: Some(prost_types::Struct {
                    fields: news_fields,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        let failures = resp.into_inner().failures;
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("ignored when 'tableId' is set")));
    }

    #[tokio::test]
    async fn create_model_yaml_options_in_ddl() {
        let p = mock_provider();
        let ctx = make_context_value("p", "d", vec!["m"]);

        let mut opt_fields = BTreeMap::new();
        opt_fields.insert("friendlyName".to_owned(), prost_string("Orders"));
        opt_fields.insert("requirePartitionFilter".to_owned(), prost_bool(true));

        let mut props_fields = BTreeMap::new();
        props_fields.insert("name".to_owned(), prost_string("m"));
        props_fields.insert(
            "sql".to_owned(),
            prost_string(
                "{{ config(materialized='table', partition_by={'field': 'ts', 'data_type': 'date'}) }} SELECT ts, id FROM t",
            ),
        );
        props_fields.insert("context".to_owned(), ctx);
        props_fields.insert("options".to_owned(), prost_struct(opt_fields));

        let resp = p
            .create_dbt_model(pulumirpc::CreateRequest {
                properties: Some(prost_types::Struct {
                    fields: props_fields,
                }),
                preview: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let output = resp.into_inner().properties.unwrap();
        let mo = crate::prost_util::get_struct_fields(&output.fields, "modelOutput").unwrap();
        let ddl = crate::prost_util::get_str(mo, "resolvedDdl").unwrap();
        assert!(
            ddl.contains("friendly_name"),
            "DDL should contain friendly_name: {}",
            ddl
        );
        assert!(
            ddl.contains("Orders"),
            "DDL should contain 'Orders': {}",
            ddl
        );
        assert!(
            ddl.contains("require_partition_filter"),
            "DDL should contain require_partition_filter: {}",
            ddl
        );
    }
}
