// generic helpers modules
pub mod generic {
    pub mod stream_ext;
}

/// Dummy main function
///
/// # Errors
/// N/A
pub fn lib_main() -> anyhow::Result<std::process::ExitCode> {
    println!("Hello World");
    Ok(std::process::ExitCode::SUCCESS)
}
