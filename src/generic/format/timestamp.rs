//! Format timestamp in localtime

use std::ffi::CStr;

use prost_types::Timestamp;

/// Format timestamp as localtime, "%Y-%m-%d %H:%M:%S"
#[must_use]
pub fn format_ts(ts: &Timestamp) -> String {
    let mut output = [0u8; 24];
    #[allow(unsafe_code)]
    unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&raw const ts.seconds, &raw mut tm);

        let _ = libc::strftime(
            output.as_mut_ptr().cast::<i8>(),
            output.len(),
            c"%F %T".as_ptr(),
            &raw const tm,
        );
    }
    match CStr::from_bytes_until_nul(&output) {
        Ok(cstr) => cstr.to_string_lossy().into(),
        Err(_err) => "N/A".into(),
    }
}

#[must_use]
pub fn format_opt_ts(ts: Option<&Timestamp>) -> String {
    if let Some(ts) = ts {
        format_ts(ts)
    } else {
        "N/A                ".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_ts() {
        assert_eq!(
            format_ts(&Timestamp {
                seconds: 1_769_981_414,
                nanos: 0
            }),
            "2026-02-01 22:30:14"
        );
    }
}
