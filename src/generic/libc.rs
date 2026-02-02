//! Access to libc / low level functions

/// Reset SIGPIPE signal handler
/// <https://github.com/rust-lang/rust/issues/62569>
/// <https://stackoverflow.com/a/65760807>
pub fn reset_sigpipe() {
    #[allow(unsafe_code)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}
