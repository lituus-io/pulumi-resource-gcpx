use std::collections::BTreeMap;

/// Table-level OPTIONS for BigQuery DDL.
#[derive(Default)]
pub struct TableOptions<'a> {
    pub require_partition_filter: Option<bool>,
    pub partition_expiration_days: Option<u32>,
    pub friendly_name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub labels: BTreeMap<&'a str, &'a str>,
    pub kms_key_name: Option<&'a str>,
    pub default_collation_name: Option<&'a str>,
    pub enable_refresh: Option<bool>,
    pub refresh_interval_minutes: Option<u32>,
    pub max_staleness: Option<&'a str>,
}

impl TableOptions<'_> {
    pub fn is_empty(&self) -> bool {
        self.require_partition_filter.is_none()
            && self.partition_expiration_days.is_none()
            && self.friendly_name.is_none()
            && self.description.is_none()
            && self.labels.is_empty()
            && self.kms_key_name.is_none()
            && self.default_collation_name.is_none()
            && self.enable_refresh.is_none()
            && self.refresh_interval_minutes.is_none()
            && self.max_staleness.is_none()
    }

    /// Render the OPTIONS(...) clause for BigQuery DDL.
    pub fn to_ddl(&self) -> String {
        let mut parts = Vec::new();

        if let Some(v) = self.require_partition_filter {
            parts.push(format!("require_partition_filter = {v}"));
        }
        if let Some(days) = self.partition_expiration_days {
            let ms = days as u64 * 86_400_000;
            parts.push(format!("partition_expiration_days = {ms}"));
        }
        if let Some(name) = self.friendly_name {
            parts.push(format!("friendly_name = \"{}\"", escape_ddl_string(name)));
        }
        if let Some(desc) = self.description {
            parts.push(format!("description = \"{}\"", escape_ddl_string(desc)));
        }
        if !self.labels.is_empty() {
            let label_pairs: Vec<String> = self
                .labels
                .iter()
                .map(|(k, v)| {
                    format!(
                        "(\"{}\", \"{}\")",
                        escape_ddl_string(k),
                        escape_ddl_string(v)
                    )
                })
                .collect();
            parts.push(format!("labels = [{}]", label_pairs.join(", ")));
        }
        if let Some(key) = self.kms_key_name {
            parts.push(format!("kms_key_name = \"{}\"", escape_ddl_string(key)));
        }
        if let Some(coll) = self.default_collation_name {
            parts.push(format!(
                "default_collation_name = \"{}\"",
                escape_ddl_string(coll)
            ));
        }
        if let Some(v) = self.enable_refresh {
            parts.push(format!("enable_refresh = {v}"));
        }
        if let Some(mins) = self.refresh_interval_minutes {
            parts.push(format!("refresh_interval_minutes = {mins}"));
        }
        if let Some(s) = self.max_staleness {
            parts.push(format!(
                "max_staleness = INTERVAL \"{}\"",
                escape_ddl_string(s)
            ));
        }

        if parts.is_empty() {
            String::new()
        } else {
            format!("OPTIONS({})", parts.join(", "))
        }
    }
}

/// Merge two TableOptions: `overrides` takes priority over `base`.
pub fn merge_options<'a>(
    base: &TableOptions<'a>,
    overrides: &TableOptions<'a>,
) -> TableOptions<'a> {
    TableOptions {
        require_partition_filter: overrides
            .require_partition_filter
            .or(base.require_partition_filter),
        partition_expiration_days: overrides
            .partition_expiration_days
            .or(base.partition_expiration_days),
        friendly_name: overrides.friendly_name.or(base.friendly_name),
        description: overrides.description.or(base.description),
        labels: if overrides.labels.is_empty() {
            base.labels.clone()
        } else {
            let mut merged = base.labels.clone();
            merged.extend(overrides.labels.iter());
            merged
        },
        kms_key_name: overrides.kms_key_name.or(base.kms_key_name),
        default_collation_name: overrides
            .default_collation_name
            .or(base.default_collation_name),
        enable_refresh: overrides.enable_refresh.or(base.enable_refresh),
        refresh_interval_minutes: overrides
            .refresh_interval_minutes
            .or(base.refresh_interval_minutes),
        max_staleness: overrides.max_staleness.or(base.max_staleness),
    }
}

/// Owned counterpart of `TableOptions` for values parsed from protobuf props.
#[derive(Debug, Default)]
pub struct OwnedTableOptions {
    pub require_partition_filter: Option<bool>,
    pub partition_expiration_days: Option<u32>,
    pub friendly_name: Option<String>,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub kms_key_name: Option<String>,
    pub default_collation_name: Option<String>,
    pub enable_refresh: Option<bool>,
    pub refresh_interval_minutes: Option<u32>,
    pub max_staleness: Option<String>,
}

impl OwnedTableOptions {
    pub fn to_table_options(&self) -> TableOptions<'_> {
        let mut labels = BTreeMap::new();
        for (k, v) in &self.labels {
            labels.insert(k.as_str(), v.as_str());
        }
        TableOptions {
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
}

fn escape_ddl_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_options() {
        let opts = TableOptions::default();
        assert!(opts.is_empty());
        assert_eq!(opts.to_ddl(), "");
    }

    #[test]
    fn partition_filter_only() {
        let opts = TableOptions {
            require_partition_filter: Some(true),
            ..Default::default()
        };
        assert!(!opts.is_empty());
        assert_eq!(opts.to_ddl(), "OPTIONS(require_partition_filter = true)");
    }

    #[test]
    fn multiple_options() {
        let opts = TableOptions {
            require_partition_filter: Some(true),
            friendly_name: Some("My Table"),
            description: Some("A test table"),
            ..Default::default()
        };
        let ddl = opts.to_ddl();
        assert!(ddl.starts_with("OPTIONS("));
        assert!(ddl.contains("require_partition_filter = true"));
        assert!(ddl.contains("friendly_name = \"My Table\""));
        assert!(ddl.contains("description = \"A test table\""));
    }

    #[test]
    fn labels() {
        let mut labels = BTreeMap::new();
        labels.insert("env", "prod");
        labels.insert("team", "analytics");
        let opts = TableOptions {
            labels,
            ..Default::default()
        };
        let ddl = opts.to_ddl();
        assert!(ddl.contains("labels = ["));
        assert!(ddl.contains("(\"env\", \"prod\")"));
        assert!(ddl.contains("(\"team\", \"analytics\")"));
    }

    #[test]
    fn kms_and_collation() {
        let opts = TableOptions {
            kms_key_name: Some("projects/p/locations/l/keyRings/kr/cryptoKeys/k"),
            default_collation_name: Some("und:ci"),
            ..Default::default()
        };
        let ddl = opts.to_ddl();
        assert!(ddl.contains("kms_key_name"));
        assert!(ddl.contains("default_collation_name = \"und:ci\""));
    }

    #[test]
    fn materialized_view_refresh() {
        let opts = TableOptions {
            enable_refresh: Some(true),
            refresh_interval_minutes: Some(30),
            max_staleness: Some("0-0 0 4:0:0"),
            ..Default::default()
        };
        let ddl = opts.to_ddl();
        assert!(ddl.contains("enable_refresh = true"));
        assert!(ddl.contains("refresh_interval_minutes = 30"));
        assert!(ddl.contains("max_staleness = INTERVAL \"0-0 0 4:0:0\""));
    }

    #[test]
    fn merge_overrides() {
        let base = TableOptions {
            require_partition_filter: Some(false),
            friendly_name: Some("Base"),
            ..Default::default()
        };
        let over = TableOptions {
            require_partition_filter: Some(true),
            description: Some("Override"),
            ..Default::default()
        };
        let merged = merge_options(&base, &over);
        assert_eq!(merged.require_partition_filter, Some(true));
        assert_eq!(merged.friendly_name, Some("Base"));
        assert_eq!(merged.description, Some("Override"));
    }

    #[test]
    fn escape_special_chars() {
        let opts = TableOptions {
            description: Some("Say \"hello\" \\ world"),
            ..Default::default()
        };
        let ddl = opts.to_ddl();
        assert!(ddl.contains("Say \\\"hello\\\" \\\\ world"));
    }
}
