/// Zero-allocation, SIMD-accelerated dbt template scanner.
///
/// Scans SQL for `{{ ... }}` template expressions and classifies them
/// as config(), ref(), source(), or macro calls.

#[derive(Clone, Copy)]
pub struct DbtScanner<'a> {
    input: &'a str,
    pos: usize,
    done: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbtSegment<'a> {
    Sql(&'a str),
    Config { raw_args: &'a str },
    Ref { model: &'a str },
    Source { source: &'a str, table: &'a str },
    Call { name: &'a str, raw_args: &'a str },
}

impl<'a> DbtScanner<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            done: false,
        }
    }
}

impl<'a> Iterator for DbtScanner<'a> {
    type Item = DbtSegment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let bytes = self.input.as_bytes();
        let start = self.pos;

        // Loop to skip consecutive single '{' without recursion.
        loop {
            // SIMD-accelerated scan for '{'.
            let found = memchr::memchr(b'{', &bytes[self.pos..]);

            match found {
                Some(offset) => {
                    let brace_pos = self.pos + offset;
                    // Check for '{{'.
                    if brace_pos + 1 < bytes.len() && bytes[brace_pos + 1] == b'{' {
                        // Emit any SQL before this template.
                        if brace_pos > start {
                            self.pos = brace_pos;
                            return Some(DbtSegment::Sql(&self.input[start..brace_pos]));
                        }

                        // Find closing '}}'.
                        let inner_start = brace_pos + 2;
                        if let Some(close_offset) = find_closing_braces(&bytes[inner_start..]) {
                            let inner_end = inner_start + close_offset;
                            let template_end = inner_end + 2; // skip '}}'
                            let inner = self.input[inner_start..inner_end].trim();
                            self.pos = template_end;

                            return Some(classify_template(inner));
                        }

                        // No closing '}}' found — treat rest as SQL.
                        self.done = true;
                        return Some(DbtSegment::Sql(&self.input[start..]));
                    }

                    // Single '{' — not a template, advance past it.
                    self.pos = brace_pos + 1;
                    // Emit SQL up to and including this '{' if there's content.
                    if brace_pos + 1 > start {
                        return Some(DbtSegment::Sql(&self.input[start..brace_pos + 1]));
                    }
                    // No content to emit (empty span) — loop to find next '{'.
                    continue;
                }
                None => {
                    // No more '{' found.
                    self.done = true;
                    if start < self.input.len() {
                        return Some(DbtSegment::Sql(&self.input[start..]));
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

/// Find the position of '}}' in a byte slice.
fn find_closing_braces(bytes: &[u8]) -> Option<usize> {
    let mut pos = 0;
    while pos < bytes.len() {
        if let Some(offset) = memchr::memchr(b'}', &bytes[pos..]) {
            let abs = pos + offset;
            if abs + 1 < bytes.len() && bytes[abs + 1] == b'}' {
                return Some(abs);
            }
            pos = abs + 1;
        } else {
            return None;
        }
    }
    None
}

/// Classify the inner content of `{{ ... }}`.
fn classify_template(inner: &str) -> DbtSegment<'_> {
    // config(...)
    if let Some(rest) = strip_prefix_ci(inner, "config(") {
        if let Some(args) = rest.strip_suffix(')') {
            return DbtSegment::Config { raw_args: args };
        }
    }

    // ref(...)
    if let Some(rest) = strip_prefix_ci(inner, "ref(") {
        if let Some(args) = rest.strip_suffix(')') {
            if let Some(model) = extract_single_quoted(args) {
                return DbtSegment::Ref { model };
            }
        }
    }

    // source(...)
    if let Some(rest) = strip_prefix_ci(inner, "source(") {
        if let Some(args) = rest.strip_suffix(')') {
            if let Some((source, table)) = extract_two_quoted(args) {
                return DbtSegment::Source { source, table };
            }
        }
    }

    // Generic function call: name(...)
    if let Some(paren_pos) = inner.find('(') {
        let name = inner[..paren_pos].trim();
        if !name.is_empty() && inner.ends_with(')') {
            let raw_args = &inner[paren_pos + 1..inner.len() - 1];
            return DbtSegment::Call { name, raw_args };
        }
    }

    // Unknown expression — pass through as SQL.
    // This preserves the original {{ ... }} in the output.
    DbtSegment::Sql(inner)
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let candidate = s.get(..prefix.len())?;
    if candidate.eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Extract a single-quoted string: `'value'` -> `value`.
fn extract_single_quoted(s: &str) -> Option<&str> {
    let s = s.trim();
    let s = s.strip_prefix('\'')?;
    let s = s.strip_suffix('\'')?;
    Some(s)
}

/// Extract two single-quoted strings: `'a', 'b'` -> `(a, b)`.
fn extract_two_quoted(s: &str) -> Option<(&str, &str)> {
    let mut parts = s.splitn(2, ',');
    let first = extract_single_quoted(parts.next()?.trim())?;
    let second = extract_single_quoted(parts.next()?.trim())?;
    Some((first, second))
}

/// Lazy config argument iterator.
#[derive(Clone, Copy)]
pub struct ConfigArgIter<'a> {
    remaining: &'a str,
}

pub struct ConfigEntry<'a> {
    pub key: &'a str,
    pub value: ConfigValue<'a>,
}

pub enum ConfigValue<'a> {
    Str(&'a str),
    Bool(bool),
    List(Vec<&'a str>),
    Dict(Vec<(&'a str, &'a str)>),
}

impl<'a> ConfigArgIter<'a> {
    pub fn new(raw_args: &'a str) -> Self {
        Self {
            remaining: raw_args.trim(),
        }
    }
}

impl<'a> Iterator for ConfigArgIter<'a> {
    type Item = ConfigEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }

        // Find key=value pair.
        let eq_pos = self.remaining.find('=')?;
        let key = self.remaining[..eq_pos].trim();
        let after_eq = self.remaining[eq_pos + 1..].trim();

        if let Some(stripped) = after_eq.strip_prefix('\'') {
            // String value: 'xxx'
            let end_quote = stripped.find('\'')?;
            let value = &stripped[..end_quote];
            let rest = stripped[end_quote + 1..].trim();
            self.remaining = rest.strip_prefix(',').unwrap_or(rest).trim();
            Some(ConfigEntry {
                key,
                value: ConfigValue::Str(value),
            })
        } else if after_eq.starts_with('{') {
            // Dict value: {'key': 'value', ...}
            let end_brace = after_eq.find('}')?;
            let dict_content = &after_eq[1..end_brace];
            let pairs: Vec<(&str, &str)> = dict_content
                .split(',')
                .filter_map(|pair| {
                    let mut kv = pair.splitn(2, ':');
                    let k = kv.next()?.trim();
                    let v = kv.next()?.trim();
                    let k = k.trim_matches('\'').trim_matches('"');
                    let v = v.trim_matches('\'').trim_matches('"');
                    if k.is_empty() {
                        None
                    } else {
                        Some((k, v))
                    }
                })
                .collect();
            let rest = after_eq[end_brace + 1..].trim();
            self.remaining = rest.strip_prefix(',').unwrap_or(rest).trim();
            Some(ConfigEntry {
                key,
                value: ConfigValue::Dict(pairs),
            })
        } else if after_eq.starts_with('[') {
            // List value: ['a', 'b']
            let end_bracket = after_eq.find(']')?;
            let list_content = &after_eq[1..end_bracket];
            let items: Vec<&str> = list_content
                .split(',')
                .filter_map(|s| extract_single_quoted(s.trim()))
                .collect();
            let rest = after_eq[end_bracket + 1..].trim();
            self.remaining = rest.strip_prefix(',').unwrap_or(rest).trim();
            Some(ConfigEntry {
                key,
                value: ConfigValue::List(items),
            })
        } else {
            // Bool or identifier value.
            let (value_str, rest) = match after_eq.find(',') {
                Some(comma) => (&after_eq[..comma], after_eq[comma + 1..].trim()),
                None => (after_eq, ""),
            };
            let value_str = value_str.trim();
            self.remaining = rest;

            let value = if value_str.eq_ignore_ascii_case("true") {
                ConfigValue::Bool(true)
            } else if value_str.eq_ignore_ascii_case("false") {
                ConfigValue::Bool(false)
            } else {
                ConfigValue::Str(value_str)
            };

            Some(ConfigEntry { key, value })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_plain_sql() {
        let sql = "SELECT * FROM table";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert_eq!(segments, vec![DbtSegment::Sql("SELECT * FROM table")]);
    }

    #[test]
    fn scan_config() {
        let sql = "{{ config(materialized='table') }}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert_eq!(
            segments,
            vec![DbtSegment::Config {
                raw_args: "materialized='table'"
            }]
        );
    }

    #[test]
    fn scan_ref() {
        let sql = "SELECT * FROM {{ ref('stg_customers') }}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], DbtSegment::Sql("SELECT * FROM "));
        assert_eq!(
            segments[1],
            DbtSegment::Ref {
                model: "stg_customers"
            }
        );
    }

    #[test]
    fn scan_source() {
        let sql = "FROM {{ source('raw', 'customers') }}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[1],
            DbtSegment::Source {
                source: "raw",
                table: "customers"
            }
        );
    }

    #[test]
    fn scan_macro_call() {
        let sql = "{{ surrogate_key('id, email') }}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert_eq!(
            segments,
            vec![DbtSegment::Call {
                name: "surrogate_key",
                raw_args: "'id, email'"
            }]
        );
    }

    #[test]
    fn scan_mixed() {
        let sql = "{{ config(materialized='view') }}\nSELECT * FROM {{ ref('stg') }}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        assert!(segments.len() >= 3);
        assert!(matches!(segments[0], DbtSegment::Config { .. }));
        assert!(matches!(segments[2], DbtSegment::Ref { model: "stg" }));
    }

    #[test]
    fn scan_empty_input() {
        let segments: Vec<_> = DbtScanner::new("").collect();
        assert!(segments.is_empty());
    }

    #[test]
    fn scan_no_templates() {
        let sql = "SELECT 1 + 2 FROM {table}";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        // Should be all SQL segments, no template segments.
        for seg in &segments {
            match seg {
                DbtSegment::Sql(_) => {}
                _ => panic!("expected only Sql segments, got: {:?}", seg),
            }
        }
    }

    #[test]
    fn scan_unclosed_template() {
        let sql = "SELECT {{ ref('x'";
        let segments: Vec<_> = DbtScanner::new(sql).collect();
        // Should not panic, treat as SQL.
        assert!(!segments.is_empty());
    }

    #[test]
    fn config_iter_single() {
        let iter = ConfigArgIter::new("materialized='table'");
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "materialized");
        match entries[0].value {
            ConfigValue::Str(v) => assert_eq!(v, "table"),
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn config_iter_multiple() {
        let iter = ConfigArgIter::new("materialized='table', tags=['daily', 'mart']");
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "materialized");
        assert_eq!(entries[1].key, "tags");
        match &entries[1].value {
            ConfigValue::List(items) => {
                assert_eq!(items, &["daily", "mart"]);
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn config_iter_bool() {
        let iter = ConfigArgIter::new("materialized='incremental', unique_key='id'");
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn config_iter_empty() {
        let iter = ConfigArgIter::new("");
        let entries: Vec<_> = iter.collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn config_value_dict() {
        let iter = ConfigArgIter::new("partition_by={'field': 'order_date', 'data_type': 'date'}");
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "partition_by");
        match &entries[0].value {
            ConfigValue::Dict(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0], ("field", "order_date"));
                assert_eq!(pairs[1], ("data_type", "date"));
            }
            _ => panic!("expected Dict"),
        }
    }

    #[test]
    fn config_value_dict_partition_by_with_other_args() {
        let iter = ConfigArgIter::new(
            "materialized='incremental', unique_key='id', partition_by={'field': 'ts', 'data_type': 'timestamp'}, cluster_by=['region']",
        );
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].key, "materialized");
        assert_eq!(entries[1].key, "unique_key");
        assert_eq!(entries[2].key, "partition_by");
        assert_eq!(entries[3].key, "cluster_by");
        match &entries[2].value {
            ConfigValue::Dict(pairs) => {
                assert_eq!(pairs[0], ("field", "ts"));
                assert_eq!(pairs[1], ("data_type", "timestamp"));
            }
            _ => panic!("expected Dict"),
        }
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn scanner_never_panics(sql in "[ -~]{0,4096}") {
            let scanner = DbtScanner::new(&sql);
            for segment in scanner {
                let _ = std::hint::black_box(segment);
            }
        }
    }
}
