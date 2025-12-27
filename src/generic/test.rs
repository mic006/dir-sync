//! Test helpers

/// Initialize logger for tests, with default log level to debug
pub fn log_init() {
    drop(
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
            .is_test(true)
            .try_init(),
    );
}
