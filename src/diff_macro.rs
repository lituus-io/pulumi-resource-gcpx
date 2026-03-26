/// Compare fields between old and new inputs, pushing changed keys into
/// `replace_keys` (trigger recreation) or `update_keys` (trigger in-place patch).
///
/// Each field is annotated with `replace` or `update` and an optional JSON key override.
///
/// # Example
/// ```ignore
/// diff_fields!(old, new, replace_keys, update_keys;
///     project => replace,
///     service_account => replace "serviceAccount",
///     sql => update,
///     time_zone => update "timeZone",
/// );
/// ```
macro_rules! diff_fields {
    ($old:expr, $new:expr, $replace:ident, $update:ident;
     $( $field:ident => $action:ident $( $key:literal )? ),+ $(,)?
    ) => {
        $(
            if $old.$field != $new.$field {
                diff_fields!(@push $action, $replace, $update, stringify!($field) $(, $key)? );
            }
        )+
    };

    (@push replace, $replace:ident, $update:ident, $default:expr) => {
        $replace.push($default);
    };
    (@push replace, $replace:ident, $update:ident, $default:expr, $key:literal) => {
        $replace.push($key);
    };
    (@push update, $replace:ident, $update:ident, $default:expr) => {
        $update.push($default);
    };
    (@push update, $replace:ident, $update:ident, $default:expr, $key:literal) => {
        $update.push($key);
    };
}

pub(crate) use diff_fields;
