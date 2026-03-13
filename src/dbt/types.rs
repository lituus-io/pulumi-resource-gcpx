use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::dbt::macros::MacroDef;

#[derive(Debug)]
pub struct ProjectInputs<'a> {
    pub gcp_project: &'a str,
    pub dataset: &'a str,
    pub sources: Vec<(&'a str, SourceInputs<'a>)>,
    pub declared_models: Vec<&'a str>,
    pub declared_macros: Vec<&'a str>,
    pub vars: Vec<(&'a str, &'a str)>,
}

#[derive(Debug)]
pub struct SourceInputs<'a> {
    pub dataset: &'a str,
    pub tables: Vec<&'a str>,
}

pub struct ProjectContext<'a> {
    pub gcp_project: &'a str,
    pub dataset: &'a str,
    pub sources: BTreeMap<String, SourceDef>,
    pub declared_models: Vec<String>,
    pub declared_macros: Vec<String>,
    pub vars: BTreeMap<String, String>,
}

pub struct SourceDef {
    pub dataset: String,
    pub tables: Vec<String>,
}

#[derive(Debug)]
pub struct MacroInputs<'a> {
    pub name: &'a str,
    pub args: Vec<&'a str>,
    pub sql: &'a str,
}

pub struct ModelInputs<'a> {
    pub context: ProjectContext<'a>,
    pub name: &'a str,
    pub sql: &'a str,
    pub model_refs: BTreeMap<String, ModelRefData>,
    pub macros: BTreeMap<String, MacroDef>,
    pub max_bytes_billed: Option<i64>,
}

/// Data stored/passed for a model reference (from modelOutput).
#[derive(Clone)]
pub struct ModelRefData {
    pub materialization: String,
    pub resolved_ctes_json: String,
    pub resolved_body: String,
    pub table_ref: String,
    pub resolved_ddl: String,
    pub resolved_sql: String,
    pub workflow_yaml: String,
}

/// Resolved SQL after template expansion.
pub struct ResolvedSql {
    pub ctes: Vec<(String, String)>,
    pub body: String,
}

impl ResolvedSql {
    pub fn to_sql(&self) -> Cow<'_, str> {
        if self.ctes.is_empty() {
            return Cow::Borrowed(&self.body);
        }

        let mut sql = String::with_capacity(
            self.ctes
                .iter()
                .map(|(n, s)| n.len() + s.len() + 10)
                .sum::<usize>()
                + self.body.len()
                + 10,
        );
        sql.push_str("WITH ");
        for (i, (name, cte_sql)) in self.ctes.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(name);
            sql.push_str(" AS (");
            sql.push_str(cte_sql);
            sql.push(')');
        }
        sql.push(' ');
        sql.push_str(&self.body);
        Cow::Owned(sql)
    }
}

/// Partition configuration parsed from config block.
pub struct PartitionConfig {
    pub field: String,
    pub data_type: String,
    pub granularity: Option<String>,
}

impl PartitionConfig {
    /// Generate the DDL partition clause fragment.
    ///
    /// BigQuery rules:
    ///   DATE column        → PARTITION BY order_date  (direct reference)
    ///   TIMESTAMP column   → PARTITION BY TIMESTAMP_TRUNC(ts, DAY)
    ///   DATETIME column    → PARTITION BY DATETIME_TRUNC(dt, DAY)
    ///   INT64 / other      → PARTITION BY col  (range partitioning handled separately)
    pub fn to_ddl(&self) -> Cow<'_, str> {
        match self.data_type.to_lowercase().as_str() {
            "timestamp" => {
                let gran = self.granularity.as_deref().unwrap_or("DAY");
                Cow::Owned(format!(
                    "TIMESTAMP_TRUNC({}, {})",
                    self.field,
                    gran.to_uppercase()
                ))
            }
            "datetime" => {
                let gran = self.granularity.as_deref().unwrap_or("DAY");
                Cow::Owned(format!(
                    "DATETIME_TRUNC({}, {})",
                    self.field,
                    gran.to_uppercase()
                ))
            }
            // DATE, INT64, and any other type: direct column reference
            _ => Cow::Borrowed(&self.field),
        }
    }
}

/// Extracted model configuration from SQL config block.
pub struct ModelConfig {
    pub materialization: String,
    pub unique_key: Option<String>,
    pub unique_key_list: Option<Vec<String>>,
    pub incremental_strategy: Option<String>,
    pub partition_by: Option<PartitionConfig>,
    pub cluster_by: Option<Vec<String>>,
    // OPTIONS fields
    pub require_partition_filter: Option<bool>,
    pub partition_expiration_days: Option<u32>,
    pub friendly_name: Option<String>,
    pub description: Option<String>,
    pub labels: Vec<(String, String)>,
    pub kms_key_name: Option<String>,
    pub default_collation_name: Option<String>,
    pub enable_refresh: Option<bool>,
    pub refresh_interval_minutes: Option<u32>,
    pub max_staleness: Option<String>,
}

impl ModelConfig {
    pub fn to_table_options(&self) -> crate::dbt::options::TableOptions<'_> {
        let mut labels = std::collections::BTreeMap::new();
        for (k, v) in &self.labels {
            labels.insert(k.as_str(), v.as_str());
        }
        crate::dbt::options::TableOptions {
            require_partition_filter: self.require_partition_filter,
            partition_expiration_days: self.partition_expiration_days,
            friendly_name: self.friendly_name.as_deref(),
            description: self.description.as_deref(),
            labels,
            kms_key_name: self.kms_key_name.as_deref(),
            default_collation_name: self.default_collation_name.as_deref(),
            enable_refresh: self.enable_refresh,
            refresh_interval_minutes: self.refresh_interval_minutes,
            max_staleness: self.max_staleness.as_deref(),
        }
    }

    /// Replace all option fields with YAML-declared values (authoritative source).
    pub fn override_options(&mut self, yaml: &crate::dbt::options::OwnedTableOptions) {
        self.require_partition_filter = yaml.require_partition_filter;
        self.partition_expiration_days = yaml.partition_expiration_days;
        self.friendly_name = yaml.friendly_name.clone();
        self.description = yaml.description.clone();
        self.labels = yaml
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.kms_key_name = yaml.kms_key_name.clone();
        self.default_collation_name = yaml.default_collation_name.clone();
        self.enable_refresh = yaml.enable_refresh;
        self.refresh_interval_minutes = yaml.refresh_interval_minutes;
        self.max_staleness = yaml.max_staleness.clone();
    }
}

/// Output data for a Model resource.
pub struct ModelOutput {
    pub materialization: String,
    pub resolved_ctes_json: String,
    pub resolved_body: String,
    pub table_ref: String,
    pub resolved_ddl: String,
    pub resolved_sql: String,
    pub workflow_yaml: String,
    pub table_id: String,
    pub scheduler_sql: String,
    /// Bytes the query would scan, populated from dry-run during check/preview.
    pub estimated_bytes_processed: Option<i64>,
    /// Human-readable cost estimate, e.g. "~$0.03 (4.8 GB)" at $6.25/TB.
    pub estimated_cost: Option<String>,
    /// Cost governance: maximum bytes billed for this model's queries.
    pub max_bytes_billed: Option<i64>,
}

/// Format a human-readable cost string from bytes processed.
/// BigQuery on-demand pricing: $6.25 per TB.
pub fn format_cost_estimate(bytes: i64) -> String {
    const PRICE_PER_TB: f64 = 6.25;
    let gb = bytes as f64 / 1_073_741_824.0;
    let tb = bytes as f64 / 1_099_511_627_776.0;
    let cost = tb * PRICE_PER_TB;
    if gb < 0.01 {
        let mb = bytes as f64 / 1_048_576.0;
        format!("~${:.4} ({:.1} MB)", cost, mb)
    } else {
        format!("~${:.2} ({:.1} GB)", cost, gb)
    }
}

/// Output data for a Macro resource.
pub struct MacroOutput {
    pub name: String,
    pub args: Vec<String>,
    pub sql: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_sql_no_ctes() {
        let r = ResolvedSql {
            ctes: vec![],
            body: "SELECT 1".to_owned(),
        };
        assert_eq!(r.to_sql(), "SELECT 1");
    }

    #[test]
    fn resolved_sql_with_ctes() {
        let r = ResolvedSql {
            ctes: vec![
                ("cte1".to_owned(), "SELECT * FROM t".to_owned()),
                ("cte2".to_owned(), "SELECT * FROM cte1".to_owned()),
            ],
            body: "SELECT * FROM cte2".to_owned(),
        };
        let sql = r.to_sql();
        assert!(sql.starts_with("WITH cte1 AS (SELECT * FROM t), cte2 AS (SELECT * FROM cte1)"));
        assert!(sql.ends_with("SELECT * FROM cte2"));
    }

    #[test]
    fn partition_config_date() {
        let pc = PartitionConfig {
            field: "order_date".to_owned(),
            data_type: "date".to_owned(),
            granularity: None,
        };
        assert_eq!(pc.to_ddl(), "order_date");
    }

    #[test]
    fn partition_config_timestamp() {
        let pc = PartitionConfig {
            field: "created_at".to_owned(),
            data_type: "timestamp".to_owned(),
            granularity: Some("day".to_owned()),
        };
        assert_eq!(pc.to_ddl(), "TIMESTAMP_TRUNC(created_at, DAY)");
    }

    #[test]
    fn partition_config_int64() {
        let pc = PartitionConfig {
            field: "id".to_owned(),
            data_type: "int64".to_owned(),
            granularity: None,
        };
        assert_eq!(pc.to_ddl(), "id");
    }
}
