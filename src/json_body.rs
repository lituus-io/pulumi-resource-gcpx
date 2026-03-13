use std::collections::BTreeMap;

/// Fluent builder for JSON API request bodies.
pub struct JsonBody(serde_json::Map<String, serde_json::Value>);

impl Default for JsonBody {
    fn default() -> Self {
        Self(serde_json::Map::new())
    }
}

impl JsonBody {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn str(mut self, key: &str, val: &str) -> Self {
        self.0
            .insert(key.into(), serde_json::Value::String(val.into()));
        self
    }

    pub fn str_opt(mut self, key: &str, val: Option<&str>) -> Self {
        if let Some(v) = val {
            self.0
                .insert(key.into(), serde_json::Value::String(v.into()));
        }
        self
    }

    pub fn num_as_str_opt(mut self, key: &str, val: Option<i64>) -> Self {
        if let Some(v) = val {
            self.0
                .insert(key.into(), serde_json::Value::String(v.to_string()));
        }
        self
    }

    pub fn bool_opt(mut self, key: &str, val: Option<bool>) -> Self {
        if let Some(v) = val {
            self.0.insert(key.into(), serde_json::Value::Bool(v));
        }
        self
    }

    pub fn labels(mut self, key: &str, labels: &BTreeMap<&str, &str>) -> Self {
        if !labels.is_empty() {
            let map: serde_json::Map<String, serde_json::Value> = labels
                .iter()
                .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                .collect();
            self.0.insert(key.into(), serde_json::Value::Object(map));
        }
        self
    }

    pub fn object(mut self, key: &str, val: serde_json::Value) -> Self {
        self.0.insert(key.into(), val);
        self
    }

    pub fn build(self) -> serde_json::Value {
        serde_json::Value::Object(self.0)
    }
}
