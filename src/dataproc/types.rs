#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    Oracle,
    Mssql,
    Mysql,
    Postgres,
}

impl DatabaseType {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("oracle") {
            Some(Self::Oracle)
        } else if s.eq_ignore_ascii_case("mssql") {
            Some(Self::Mssql)
        } else if s.eq_ignore_ascii_case("mysql") {
            Some(Self::Mysql)
        } else if s.eq_ignore_ascii_case("postgres") {
            Some(Self::Postgres)
        } else {
            None
        }
    }

    pub fn jdbc_prefix(&self) -> &'static str {
        match self {
            Self::Oracle => "jdbc:oracle:",
            Self::Mssql => "jdbc:sqlserver:",
            Self::Mysql => "jdbc:mysql:",
            Self::Postgres => "jdbc:postgresql:",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Oracle => "oracle",
            Self::Mssql => "mssql",
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationType {
    Gcs,
    BigQuery,
}

impl DestinationType {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("gcs") {
            Some(Self::Gcs)
        } else if s.eq_ignore_ascii_case("bigquery") {
            Some(Self::BigQuery)
        } else {
            None
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Gcs => "gcs",
            Self::BigQuery => "bigquery",
        }
    }
}

#[derive(Debug)]
pub struct IngestJobInputs<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub name: &'a str,
    pub database_type: DatabaseType,
    pub connection_string: &'a str,
    pub secret: &'a str,
    pub query: &'a str,
    pub destination: DestinationType,
    pub landing_bucket: Option<&'a str>,
    pub dest_project: Option<&'a str>,
    pub dest_dataset: Option<&'a str>,
    pub dest_table: Option<&'a str>,
    pub image_uri: &'a str,
    pub script_uri: &'a str,
    pub jar_uris: Vec<&'a str>,
    pub runtime_version: &'a str,
    pub service_account: &'a str,
    pub staging_bucket: &'a str,
    pub process_bucket: &'a str,
    pub subnetwork_uri: &'a str,
    pub network_tags: Vec<&'a str>,
    pub schedule: &'a str,
    pub time_zone: &'a str,
    pub paused: Option<bool>,
    pub description: Option<&'a str>,
    pub retry_count: Option<i32>,
    pub attempt_deadline: Option<&'a str>,
}

#[derive(Debug)]
pub struct ExportJobInputs<'a> {
    pub project: &'a str,
    pub region: &'a str,
    pub name: &'a str,
    pub source_dataset: &'a str,
    pub source_table: &'a str,
    pub database_type: DatabaseType,
    pub connection_string: &'a str,
    pub secret: &'a str,
    pub dest_table: &'a str,
    pub image_uri: &'a str,
    pub script_uri: &'a str,
    pub jar_uris: Vec<&'a str>,
    pub runtime_version: &'a str,
    pub service_account: &'a str,
    pub staging_bucket: &'a str,
    pub process_bucket: &'a str,
    pub subnetwork_uri: &'a str,
    pub network_tags: Vec<&'a str>,
    pub schedule: &'a str,
    pub time_zone: &'a str,
    pub paused: Option<bool>,
    pub description: Option<&'a str>,
    pub retry_count: Option<i32>,
    pub attempt_deadline: Option<&'a str>,
}

pub struct DataprocJobState {
    pub workflow_name: String,
    pub scheduler_job_name: String,
    pub state: String,
    pub next_run_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_from_str() {
        assert_eq!(DatabaseType::parse("oracle"), Some(DatabaseType::Oracle));
        assert_eq!(DatabaseType::parse("ORACLE"), Some(DatabaseType::Oracle));
        assert_eq!(DatabaseType::parse("Oracle"), Some(DatabaseType::Oracle));
        assert_eq!(DatabaseType::parse("mssql"), Some(DatabaseType::Mssql));
        assert_eq!(DatabaseType::parse("mysql"), Some(DatabaseType::Mysql));
        assert_eq!(
            DatabaseType::parse("postgres"),
            Some(DatabaseType::Postgres)
        );
    }

    #[test]
    fn test_database_type_jdbc_prefix() {
        assert_eq!(DatabaseType::Oracle.jdbc_prefix(), "jdbc:oracle:");
        assert_eq!(DatabaseType::Mssql.jdbc_prefix(), "jdbc:sqlserver:");
        assert_eq!(DatabaseType::Mysql.jdbc_prefix(), "jdbc:mysql:");
        assert_eq!(DatabaseType::Postgres.jdbc_prefix(), "jdbc:postgresql:");
    }

    #[test]
    fn test_database_type_unknown_returns_none() {
        assert_eq!(DatabaseType::parse("sqlite"), None);
        assert_eq!(DatabaseType::parse(""), None);
        assert_eq!(DatabaseType::parse("db2"), None);
    }

    #[test]
    fn test_destination_type_from_str() {
        assert_eq!(DestinationType::parse("gcs"), Some(DestinationType::Gcs));
        assert_eq!(DestinationType::parse("GCS"), Some(DestinationType::Gcs));
        assert_eq!(
            DestinationType::parse("bigquery"),
            Some(DestinationType::BigQuery)
        );
        assert_eq!(
            DestinationType::parse("BigQuery"),
            Some(DestinationType::BigQuery)
        );
    }

    #[test]
    fn test_destination_type_unknown_returns_none() {
        assert_eq!(DestinationType::parse("s3"), None);
        assert_eq!(DestinationType::parse(""), None);
    }
}
