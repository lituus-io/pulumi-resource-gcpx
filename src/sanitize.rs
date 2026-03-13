/// Escape a BigQuery identifier for use inside backtick-delimited references.
///
/// BigQuery identifiers are quoted with backticks (`` `project.dataset.table` ``).
/// If a component contains a literal backtick or backslash, it must be escaped
/// to prevent identifier breakout.
///
/// This function escapes `\` → `\\` and `` ` `` → `` \` `` per BigQuery's
/// identifier quoting rules.
#[inline]
pub fn escape_bq_ident(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains('`') || s.contains('\\') {
        std::borrow::Cow::Owned(s.replace('\\', "\\\\").replace('`', "\\`"))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Build a backtick-quoted `project.dataset.table` reference with proper escaping.
#[inline]
pub fn bq_table_ref(project: &str, dataset: &str, table: &str) -> String {
    format!(
        "`{}.{}.{}`",
        escape_bq_ident(project),
        escape_bq_ident(dataset),
        escape_bq_ident(table),
    )
}

/// Build a backtick-quoted column reference with proper escaping.
#[inline]
pub fn bq_col_ref(name: &str) -> String {
    format!("`{}`", escape_bq_ident(name))
}

/// URL-encode a path segment for GCP API URLs.
///
/// Encodes characters that are unsafe in URL path segments (/, ?, #, etc.)
/// using percent-encoding per RFC 3986.
pub fn encode_path_segment(s: &str) -> std::borrow::Cow<'_, str> {
    // Fast path: most GCP identifiers are alphanumeric + hyphen + underscore + dot
    if s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut encoded = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", b));
            }
        }
    }
    std::borrow::Cow::Owned(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_escaping_needed() {
        assert_eq!(escape_bq_ident("my_table"), "my_table");
        assert!(matches!(
            escape_bq_ident("my_table"),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn escapes_backtick() {
        assert_eq!(escape_bq_ident("my`table"), "my\\`table");
    }

    #[test]
    fn escapes_backslash() {
        assert_eq!(escape_bq_ident("my\\table"), "my\\\\table");
    }

    #[test]
    fn escapes_both() {
        assert_eq!(escape_bq_ident("a`b\\c"), "a\\`b\\\\c");
    }

    #[test]
    fn bq_table_ref_basic() {
        assert_eq!(bq_table_ref("proj", "ds", "tbl"), "`proj.ds.tbl`");
    }

    #[test]
    fn bq_table_ref_escapes() {
        assert_eq!(bq_table_ref("p`roj", "ds", "tbl"), "`p\\`roj.ds.tbl`");
    }

    #[test]
    fn bq_col_ref_basic() {
        assert_eq!(bq_col_ref("col_name"), "`col_name`");
    }

    #[test]
    fn bq_col_ref_escapes() {
        assert_eq!(bq_col_ref("col`name"), "`col\\`name`");
    }

    #[test]
    fn encode_path_segment_passthrough() {
        assert_eq!(encode_path_segment("my-project_123"), "my-project_123");
        assert!(matches!(
            encode_path_segment("my-project_123"),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn encode_path_segment_encodes_slash() {
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
    }

    #[test]
    fn encode_path_segment_encodes_special() {
        assert_eq!(encode_path_segment("a?b#c"), "a%3Fb%23c");
    }

    #[test]
    fn encode_path_segment_encodes_space() {
        assert_eq!(encode_path_segment("a b"), "a%20b");
    }
}
