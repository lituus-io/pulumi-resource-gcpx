use std::collections::BTreeMap;

use crate::prost_util::{prost_bool, prost_list, prost_number, prost_string, prost_struct};

/// Fluent builder for prost output construction.
#[derive(Default)]
pub struct OutputBuilder(BTreeMap<String, prost_types::Value>);

impl OutputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn str(mut self, key: &str, val: &str) -> Self {
        self.0.insert(key.to_owned(), prost_string(val));
        self
    }

    pub fn str_opt(mut self, key: &str, val: Option<&str>) -> Self {
        if let Some(v) = val {
            self.0.insert(key.to_owned(), prost_string(v));
        }
        self
    }

    pub fn num(mut self, key: &str, val: f64) -> Self {
        self.0.insert(key.to_owned(), prost_number(val));
        self
    }

    pub fn num_opt(mut self, key: &str, val: Option<i64>) -> Self {
        if let Some(v) = val {
            self.0.insert(key.to_owned(), prost_number(v as f64));
        }
        self
    }

    pub fn bool_val(mut self, key: &str, val: bool) -> Self {
        self.0.insert(key.to_owned(), prost_bool(val));
        self
    }

    pub fn bool_opt(mut self, key: &str, val: Option<bool>) -> Self {
        if let Some(v) = val {
            self.0.insert(key.to_owned(), prost_bool(v));
        }
        self
    }

    pub fn list(mut self, key: &str, items: Vec<prost_types::Value>) -> Self {
        self.0.insert(key.to_owned(), prost_list(items));
        self
    }

    pub fn str_list(mut self, key: &str, items: &[&str]) -> Self {
        let values = items.iter().map(|s| prost_string(s)).collect();
        self.0.insert(key.to_owned(), prost_list(values));
        self
    }

    pub fn nested(mut self, key: &str, inner: prost_types::Struct) -> Self {
        self.0.insert(key.to_owned(), prost_struct(inner.fields));
        self
    }

    pub fn nested_opt(self, key: &str, inner: Option<prost_types::Struct>) -> Self {
        if let Some(s) = inner {
            self.nested(key, s)
        } else {
            self
        }
    }

    pub fn labels(mut self, key: &str, labels: &BTreeMap<&str, &str>) -> Self {
        if !labels.is_empty() {
            let mut label_fields = BTreeMap::new();
            for (k, v) in labels {
                label_fields.insert(k.to_string(), prost_string(v));
            }
            self.0.insert(key.to_owned(), prost_struct(label_fields));
        }
        self
    }

    pub fn build(self) -> prost_types::Struct {
        prost_types::Struct { fields: self.0 }
    }

    pub fn build_value(self) -> prost_types::Value {
        prost_struct(self.0)
    }
}
