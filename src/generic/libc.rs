//! Access to libc / low level functions

use std::ffi::CStr;

use prost_types::Timestamp;

/// Reset SIGPIPE signal handler
/// <https://github.com/rust-lang/rust/issues/62569>
/// <https://stackoverflow.com/a/65760807>
pub fn reset_sigpipe() {
    #[allow(unsafe_code)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

/// Format timestamp as localtime, "%Y-%m-%d %H:%M:%S"
#[must_use]
pub fn strftime_local(ts: &Timestamp) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strftime_local() {
        assert_eq!(
            strftime_local(&Timestamp {
                seconds: 1_769_981_414,
                nanos: 0
            }),
            "2026-02-01 22:30:14"
        );
    }
}
